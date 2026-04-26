import { getPool } from "/opt/nodejs/index";
import type { SQSEvent } from 'aws-lambda';

interface MatchEndedMessage {
  matchId: string;
  seasonId: string;
  tournamentId: string;
}

export const lambdaHandler = async (event: SQSEvent): Promise<void> => {
  const pool = getPool();
  const client = await pool.connect();

  try {
    for (const record of event.Records) {
      const message: MatchEndedMessage = JSON.parse(record.body);
      const { matchId, seasonId, tournamentId } = message;

      console.log('Resolving matchups for finished match:', matchId);

      // Single query that:
      // 1. Finds all match_nums this match belongs to (home or away)
      // 2. Finds all leagues using this season/tournament
      // 3. Finds all fantasy_matchups at those match_nums
      // 4. Checks if ALL IRL matches for that match_num are FINISHED
      // 5. Calculates fantasy scores per side
      // 6. Updates fantasy_winner_team_instance_id for resolved matchups
      const sql = `
WITH
-- 1. Get the match_nums this match is associated with
finished_match AS (
    SELECT id, home_match_num, away_match_num
    FROM irldata.match_info
    WHERE id = $1
),

-- 2. All active leagues on this season/tournament
active_leagues AS (
    SELECT l.id AS league_id, l.season_id, l.tournament_id
    FROM fantasydata.leagues l
    WHERE l.season_id = $2
      AND l.tournament_id = $3
      AND l.status = 'active'
),

-- 3. Matchups at the relevant match_nums (could be home_match_num or away_match_num)
candidate_matchups AS (
    SELECT DISTINCT
        fm.id AS matchup_id,
        fm.match_num,
        fm.league_id,
        fm.fantasy_team_instance1_id,
        fm.fantasy_team_instance2_id,
        al.season_id,
        al.tournament_id
    FROM fantasydata.fantasy_matchups fm
    JOIN active_leagues al ON al.league_id = fm.league_id
    CROSS JOIN finished_match fmi
    WHERE fm.fantasy_winner_team_instance_id IS NULL
      AND (fm.match_num = fmi.home_match_num OR fm.match_num = fmi.away_match_num)
),

-- 4. For each candidate matchup, find the IRL teams in the rosters
matchup_irl_teams AS (
    SELECT DISTINCT
        cm.matchup_id, cm.match_num, cm.season_id, cm.tournament_id,
        psi.team_id
    FROM candidate_matchups cm
    JOIN fantasydata.fantasy_team_instance ti
        ON ti.id IN (cm.fantasy_team_instance1_id, cm.fantasy_team_instance2_id)
    CROSS JOIN LATERAL (VALUES
        (ti.bat1),(ti.bat2),(ti.wicket1),
        (ti.bowl1),(ti.bowl2),(ti.bowl3),
        (ti.all1),(ti.flex1)
    ) AS u(player_id)
    JOIN irldata.player_season_info psi
        ON psi.player_id = u.player_id
       AND psi.season_id = cm.season_id
       AND psi.tournament_id = cm.tournament_id
    WHERE u.player_id IS NOT NULL
),

-- 5. Check which matchups have ALL their IRL matches FINISHED
fully_finished_matchups AS (
    SELECT mt.matchup_id
    FROM matchup_irl_teams mt
    JOIN irldata.match_info mi
        ON mi.season_id = mt.season_id
       AND mi.tournament_id = mt.tournament_id
       AND (
            (mi.home_team_id = mt.team_id AND mi.home_match_num = mt.match_num)
            OR (mi.away_team_id = mt.team_id AND mi.away_match_num = mt.match_num)
       )
    GROUP BY mt.matchup_id
    HAVING COUNT(*) FILTER (WHERE mi.status != 'FINISHED') = 0
),

-- 6. Only process matchups that are fully finished
resolvable_matchups AS (
    SELECT cm.*
    FROM candidate_matchups cm
    JOIN fully_finished_matchups ff ON ff.matchup_id = cm.matchup_id
),

-- 7. League scoring rules for resolvable matchups
league_rules AS (
    SELECT DISTINCT r.*
    FROM fantasydata.league_scoring_rules r
    JOIN resolvable_matchups rm ON rm.league_id = r.league_id
),

-- 8. Unpivot rosters (starters only, not bench)
team_rosters AS (
    SELECT
        rm.matchup_id, rm.league_id, rm.match_num,
        rm.season_id, rm.tournament_id,
        side.team_side, side.instance_id,
        side.captain, side.vice_captain,
        u.player_id
    FROM resolvable_matchups rm
    CROSS JOIN LATERAL (VALUES
        (1, rm.fantasy_team_instance1_id),
        (2, rm.fantasy_team_instance2_id)
    ) AS side(team_side, instance_id)
    JOIN fantasydata.fantasy_team_instance ti ON ti.id = side.instance_id
    CROSS JOIN LATERAL (VALUES
        (ti.bat1),(ti.bat2),(ti.wicket1),
        (ti.bowl1),(ti.bowl2),(ti.bowl3),
        (ti.all1),(ti.flex1)
    ) AS u(player_id)
    CROSS JOIN LATERAL (VALUES (ti.captain, ti.vice_captain)) AS cv(captain, vice_captain)
    WHERE u.player_id IS NOT NULL
),

-- 9. Resolve performances
resolved_performances AS (
    SELECT
        tr.matchup_id, tr.league_id, tr.team_side, tr.instance_id,
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

-- 10. Derived stats
player_stats_calc AS (
    SELECT rp.*,
        CASE WHEN COALESCE(rp.balls_faced, 0) > 0
             THEN (rp.runs_scored * 100.0 / rp.balls_faced)::NUMERIC ELSE 0 END AS strike_rate,
        CASE WHEN COALESCE(rp.balls_bowled, 0) > 0
             THEN (rp.runs_conceded / (rp.balls_bowled / 6.0))::NUMERIC ELSE 0 END AS economy
    FROM resolved_performances rp
),

-- 11. Standard scoring
standard_points AS (
    SELECT ps.player_id, ps.instance_id, ps.league_id,
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
    GROUP BY ps.player_id, ps.instance_id, ps.league_id
),

-- 12. Band scoring
band_points AS (
    SELECT ps.player_id, ps.instance_id, ps.league_id,
        SUM(r.flat_points) AS total_band_points
    FROM player_stats_calc ps
    JOIN league_rules r ON r.league_id = ps.league_id AND r.mode = 'band'
    WHERE
        (r.stat = 'Strike Rate' AND COALESCE(ps.balls_faced, 0) > 0 AND r.band @> ps.strike_rate)
        OR
        (r.stat = 'Economy' AND COALESCE(ps.balls_bowled, 0) > 0 AND r.band @> ps.economy)
    GROUP BY ps.player_id, ps.instance_id, ps.league_id
),

-- 13. Per-player fantasy scores with captain/vc multipliers
individual_scores AS (
    SELECT
        ps.matchup_id, ps.team_side, ps.instance_id, ps.player_id, ps.league_id,
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
        ON sp.player_id = ps.player_id AND sp.instance_id = ps.instance_id AND sp.league_id = ps.league_id
    LEFT JOIN band_points bp
        ON bp.player_id = ps.player_id AND bp.instance_id = ps.instance_id AND bp.league_id = ps.league_id
),

-- 14. Aggregate per matchup side
matchup_scores AS (
    SELECT
        rm.matchup_id,
        rm.fantasy_team_instance1_id,
        rm.fantasy_team_instance2_id,
        COALESCE(SUM(CASE WHEN ind.team_side = 1 THEN ind.final_player_score ELSE 0 END), 0) AS team1_score,
        COALESCE(SUM(CASE WHEN ind.team_side = 2 THEN ind.final_player_score ELSE 0 END), 0) AS team2_score
    FROM resolvable_matchups rm
    LEFT JOIN individual_scores ind ON ind.matchup_id = rm.matchup_id
    GROUP BY rm.matchup_id, rm.fantasy_team_instance1_id, rm.fantasy_team_instance2_id
)

-- 15. Update winners
UPDATE fantasydata.fantasy_matchups fm
SET fantasy_winner_team_instance_id = CASE
    WHEN ms.team1_score > ms.team2_score THEN ms.fantasy_team_instance1_id
    WHEN ms.team2_score > ms.team1_score THEN ms.fantasy_team_instance2_id
    ELSE ms.fantasy_team_instance1_id -- tie-breaker: team 1 (home) wins
END
FROM matchup_scores ms
WHERE fm.id = ms.matchup_id;
      `;

      const result = await client.query(sql, [matchId, seasonId, tournamentId]);
      console.log(`Resolved ${result.rowCount} matchup(s) for match ${matchId}`);
    }
  } catch (err) {
    console.error('Matchup resolution failed:', err);
    throw err; // Let SQS retry
  } finally {
    client.release();
  }
};
