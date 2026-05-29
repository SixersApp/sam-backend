-- Centralize matchup scoring into a single SQL function.
--
-- fantasydata.compute_matchup_scores(uuid[]) returns one row per input matchup
-- with team1_score / team2_score. Each matchup is scored against its own
-- league's scoring rules (the function joins league_scoring_rules on the
-- matchup's league_id, so calling with matchups from multiple leagues works
-- correctly).
--
-- Used by:
--   - matchup-resolver/app.ts  (to determine winners on IRL match end)
--   - league-functions/app.ts  (to compute live leaderboard stats)

CREATE OR REPLACE FUNCTION fantasydata.compute_matchup_scores(_matchup_ids uuid[])
RETURNS TABLE(matchup_id uuid, team1_score NUMERIC, team2_score NUMERIC)
LANGUAGE sql
STABLE
AS $$
    WITH
    target_matchups AS (
        SELECT
            fm.id AS matchup_id,
            fm.match_num,
            fm.league_id,
            fm.fantasy_team_instance1_id,
            fm.fantasy_team_instance2_id,
            l.season_id,
            l.tournament_id
        FROM fantasydata.fantasy_matchups fm
        JOIN fantasydata.leagues l ON l.id = fm.league_id
        WHERE fm.id = ANY(_matchup_ids)
    ),

    league_rules AS (
        SELECT r.*
        FROM fantasydata.league_scoring_rules r
        WHERE r.league_id IN (SELECT DISTINCT league_id FROM target_matchups)
    ),

    team_rosters AS (
        SELECT
            tm.matchup_id, tm.match_num, tm.league_id,
            tm.season_id, tm.tournament_id,
            side.team_side, side.instance_id,
            ti.captain, ti.vice_captain,
            u.player_id
        FROM target_matchups tm
        CROSS JOIN LATERAL (VALUES
            (1, tm.fantasy_team_instance1_id),
            (2, tm.fantasy_team_instance2_id)
        ) AS side(team_side, instance_id)
        JOIN fantasydata.fantasy_team_instance ti ON ti.id = side.instance_id
        CROSS JOIN LATERAL (VALUES
            (ti.bat1),(ti.bat2),(ti.wicket1),
            (ti.bowl1),(ti.bowl2),(ti.bowl3),
            (ti.all1),(ti.flex1)
        ) AS u(player_id)
        WHERE u.player_id IS NOT NULL
    ),

    resolved_performances AS (
        SELECT
            tr.matchup_id, tr.team_side, tr.instance_id, tr.league_id,
            tr.player_id, tr.captain, tr.vice_captain,
            pp.runs_scored, pp.balls_faced, pp.fours, pp.sixes,
            pp.balls_bowled, pp.runs_conceded, pp.wickets_taken,
            pp.catches, pp.run_outs, pp.catches_dropped, pp.not_out
        FROM team_rosters tr
        JOIN irldata.player_season_info psi
            ON psi.player_id = tr.player_id
           AND psi.season_id = tr.season_id
           AND psi.tournament_id = tr.tournament_id
        JOIN irldata.match_info mi
            ON mi.season_id = tr.season_id
           AND mi.tournament_id = tr.tournament_id
           AND (
                (mi.home_team_id = psi.team_id AND mi.home_match_num = tr.match_num)
                OR (mi.away_team_id = psi.team_id AND mi.away_match_num = tr.match_num)
           )
        JOIN irldata.player_performance pp
            ON pp.match_id = mi.id
           AND pp.player_season_id = psi.id
    ),

    player_stats_calc AS (
        SELECT rp.*,
            CASE WHEN COALESCE(rp.balls_faced, 0) > 0
                 THEN (rp.runs_scored * 100.0 / rp.balls_faced)::NUMERIC ELSE 0 END AS strike_rate,
            CASE WHEN COALESCE(rp.balls_bowled, 0) > 0
                 THEN (rp.runs_conceded / (rp.balls_bowled / 6.0))::NUMERIC ELSE 0 END AS economy
        FROM resolved_performances rp
    ),

    standard_points AS (
        SELECT ps.matchup_id, ps.instance_id, ps.player_id,
            SUM(CASE
                WHEN r.stat = 'Points per run' THEN COALESCE(ps.runs_scored, 0) * r.per_unit_points
                WHEN r.stat = 'Bonus per 4' THEN COALESCE(ps.fours, 0) * r.per_unit_points
                WHEN r.stat = 'Bonus per 6' THEN COALESCE(ps.sixes, 0) * r.per_unit_points
                WHEN r.stat = 'Bonus per half-century' AND COALESCE(ps.runs_scored, 0) >= 50 THEN r.flat_points
                WHEN r.stat = 'Bonus per century' AND COALESCE(ps.runs_scored, 0) >= 100 THEN r.flat_points
                WHEN r.stat = 'Duck-out Penalty' AND COALESCE(ps.runs_scored, 0) = 0
                     AND COALESCE(ps.balls_faced, 0) > 0 THEN r.flat_points
                WHEN r.stat = 'Points per Wicket' THEN COALESCE(ps.wickets_taken, 0) * r.per_unit_points
                WHEN r.stat = '3-Wicket Bonus' THEN FLOOR(COALESCE(ps.wickets_taken, 0) / 3.0) * r.per_unit_points
                WHEN r.stat = '5-Wicket Bonus' THEN FLOOR(COALESCE(ps.wickets_taken, 0) / 5.0) * r.per_unit_points
                WHEN r.stat = 'Points per catch' THEN COALESCE(ps.catches, 0) * r.per_unit_points
                WHEN r.stat = '3-Catches bonus' THEN FLOOR(COALESCE(ps.catches, 0) / 3.0) * r.per_unit_points
                WHEN r.stat = 'Run Out' THEN COALESCE(ps.run_outs, 0) * r.per_unit_points
                WHEN r.stat = 'Dropped Catch' THEN COALESCE(ps.catches_dropped, 0) * r.per_unit_points
                ELSE 0
            END) AS total_std_points
        FROM player_stats_calc ps
        JOIN league_rules r ON r.league_id = ps.league_id
        WHERE r.mode != 'band' AND r.category != 'leadership'
        GROUP BY ps.matchup_id, ps.instance_id, ps.player_id
    ),

    band_points AS (
        SELECT ps.matchup_id, ps.instance_id, ps.player_id,
            SUM(r.flat_points) AS total_band_points
        FROM player_stats_calc ps
        JOIN league_rules r ON r.league_id = ps.league_id AND r.mode = 'band'
        WHERE
            (r.stat = 'Strike Rate' AND COALESCE(ps.balls_faced, 0) > 0 AND r.band @> ps.strike_rate)
            OR
            (r.stat = 'Economy' AND COALESCE(ps.balls_bowled, 0) > 0 AND r.band @> ps.economy)
        GROUP BY ps.matchup_id, ps.instance_id, ps.player_id
    ),

    individual_scores AS (
        SELECT
            ps.matchup_id, ps.team_side, ps.instance_id, ps.player_id,
            (COALESCE(sp.total_std_points, 0) + COALESCE(bp.total_band_points, 0))
            * COALESCE((
                SELECT multiplier FROM league_rules
                WHERE league_id = ps.league_id AND stat = 'Captaincy Multiplier'
                  AND ps.player_id = ps.captain
            ), 1)
            * COALESCE((
                SELECT multiplier FROM league_rules
                WHERE league_id = ps.league_id AND stat = 'Vice Captaincy Multiplier'
                  AND ps.player_id = ps.vice_captain
            ), 1) AS final_player_score
        FROM player_stats_calc ps
        LEFT JOIN standard_points sp
            ON sp.matchup_id = ps.matchup_id
           AND sp.instance_id = ps.instance_id
           AND sp.player_id = ps.player_id
        LEFT JOIN band_points bp
            ON bp.matchup_id = ps.matchup_id
           AND bp.instance_id = ps.instance_id
           AND bp.player_id = ps.player_id
    )

    SELECT
        tm.matchup_id,
        COALESCE(SUM(CASE WHEN ind.team_side = 1 THEN ind.final_player_score ELSE 0 END), 0)::NUMERIC AS team1_score,
        COALESCE(SUM(CASE WHEN ind.team_side = 2 THEN ind.final_player_score ELSE 0 END), 0)::NUMERIC AS team2_score
    FROM target_matchups tm
    LEFT JOIN individual_scores ind ON ind.matchup_id = tm.matchup_id
    GROUP BY tm.matchup_id;
$$;
