import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   GET ALL LIVE / UPCOMING MATCHES FOR USER (HOME FEED)
   GET /matches/feed
   ======================================================================================= */
app.get("/matchups/feed", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      WITH
-- 1. User's teams and their leagues
user_teams AS (
    SELECT ft.id AS team_id, ft.league_id
    FROM fantasydata.fantasy_teams ft
    WHERE ft.user_id = $1
),

-- 2. User's active leagues
user_leagues AS (
    SELECT l.id AS league_id, l.season_id, l.tournament_id
    FROM fantasydata.leagues l
    WHERE l.status = 'active'
      AND l.id IN (SELECT DISTINCT league_id FROM user_teams)
),

-- 3. All matchups in user's active leagues with roster context
league_matchups AS (
    SELECT m.id AS matchup_id, m.match_num, m.league_id,
           m.fantasy_team_instance1_id, m.fantasy_team_instance2_id,
           l.season_id, l.tournament_id
    FROM fantasydata.fantasy_matchups m
    JOIN user_leagues ul ON ul.league_id = m.league_id
    JOIN fantasydata.leagues l ON l.id = m.league_id
),

-- 4. Distinct IRL teams represented in each matchup's starting lineup
matchup_irl_teams AS (
    SELECT DISTINCT lm.matchup_id, lm.match_num, lm.league_id,
           lm.season_id, lm.tournament_id, psi.team_id
    FROM league_matchups lm
    JOIN fantasydata.fantasy_team_instance ti1 ON ti1.id = lm.fantasy_team_instance1_id
    JOIN fantasydata.fantasy_team_instance ti2 ON ti2.id = lm.fantasy_team_instance2_id
    CROSS JOIN LATERAL (VALUES
        (ti1.bat1), (ti1.bat2), (ti1.wicket1),
        (ti1.bowl1), (ti1.bowl2), (ti1.bowl3),
        (ti1.all1), (ti1.flex1),
        (ti2.bat1), (ti2.bat2), (ti2.wicket1),
        (ti2.bowl1), (ti2.bowl2), (ti2.bowl3),
        (ti2.all1), (ti2.flex1)
    ) AS u(player_id)
    JOIN irldata.player_season_info psi
        ON psi.player_id = u.player_id
       AND psi.season_id = lm.season_id
       AND psi.tournament_id = lm.tournament_id
    WHERE u.player_id IS NOT NULL
),

-- 5. IRL match statuses for each matchup's roster players at that match_num
matchup_match_statuses AS (
    SELECT DISTINCT mt.matchup_id, mt.league_id, mt.match_num, mi.status
    FROM matchup_irl_teams mt
    JOIN irldata.match_info mi
        ON mi.tournament_id = mt.tournament_id
       AND mi.season_id = mt.season_id
       AND (
            (mi.home_team_id = mt.team_id AND mi.home_match_num = mt.match_num)
            OR (mi.away_team_id = mt.team_id AND mi.away_match_num = mt.match_num)
       )
),

-- 6. Active match_nums: any matchup where a roster player has a LIVE match,
--    or a mix of FINISHED and NS/LIVE matches
active_matchup_weeks AS (
    SELECT DISTINCT league_id, match_num
    FROM matchup_match_statuses
    GROUP BY matchup_id, league_id, match_num
    HAVING
        COUNT(*) FILTER (WHERE status = 'LIVE') > 0
        OR (
            COUNT(*) FILTER (WHERE status IN ('FINISHED', 'ABAN.')) > 0
            AND COUNT(*) FILTER (WHERE status IN ('NS', 'LIVE')) > 0
        )
),

-- 7. Completed weeks (all IRL matches finished) for fallback
completed_weeks AS (
    SELECT DISTINCT mms.league_id, mms.match_num
    FROM matchup_match_statuses mms
    GROUP BY mms.matchup_id, mms.league_id, mms.match_num
    HAVING COUNT(*) FILTER (WHERE status IN ('NS', 'LIVE')) = 0
),

-- 8. Target weeks: active + next, OR fallback to next after latest completed
target_weeks AS (
    SELECT league_id, match_num FROM active_matchup_weeks
    UNION
    SELECT sub.league_id, sub.next_num
    FROM (
        SELECT league_id, MAX(match_num) + 1 AS next_num
        FROM active_matchup_weeks
        GROUP BY league_id
    ) sub
    WHERE EXISTS (
        SELECT 1 FROM fantasydata.fantasy_matchups fm
        WHERE fm.league_id = sub.league_id AND fm.match_num = sub.next_num
    )
    UNION
    -- Fallback: no active weeks, show next game after latest completed
    SELECT lc.league_id, lc.max_completed + 1
    FROM (
        SELECT league_id, MAX(match_num) AS max_completed
        FROM completed_weeks
        GROUP BY league_id
    ) lc
    WHERE NOT EXISTS (SELECT 1 FROM active_matchup_weeks aw WHERE aw.league_id = lc.league_id)
      AND EXISTS (
        SELECT 1 FROM fantasydata.fantasy_matchups fm
        WHERE fm.league_id = lc.league_id AND fm.match_num = lc.max_completed + 1
    )
),

-- 7. All matchups in target weeks
candidate_matchups AS (
    SELECT
        m.id AS matchup_id,
        m.match_num,
        m.league_id,
        m.fantasy_team_instance1_id,
        m.fantasy_team_instance2_id,
        ti1.fantasy_team_id AS fantasy_team1_id,
        ti2.fantasy_team_id AS fantasy_team2_id,
        ti1.captain AS captain1,
        ti1.vice_captain AS vice_captain1,
        ti2.captain AS captain2,
        ti2.vice_captain AS vice_captain2,
        l.season_id,
        l.tournament_id
    FROM fantasydata.fantasy_matchups m
    JOIN fantasydata.leagues l ON l.id = m.league_id
    JOIN fantasydata.fantasy_team_instance ti1 ON ti1.id = m.fantasy_team_instance1_id
    JOIN fantasydata.fantasy_team_instance ti2 ON ti2.id = m.fantasy_team_instance2_id
    JOIN target_weeks tw ON tw.league_id = m.league_id AND tw.match_num = m.match_num
),

-- 8. Unpivot team rosters with player info
roster_players AS (
    SELECT
        cm.matchup_id,
        cm.league_id,
        cm.match_num,
        cm.season_id,
        cm.tournament_id,
        side.team_side,
        side.instance_id,
        side.fantasy_team_id,
        u.slot,
        u.player_id
    FROM candidate_matchups cm
    CROSS JOIN LATERAL (
        VALUES
            (1, cm.fantasy_team_instance1_id, cm.fantasy_team1_id),
            (2, cm.fantasy_team_instance2_id, cm.fantasy_team2_id)
    ) AS side(team_side, instance_id, fantasy_team_id)
    JOIN fantasydata.fantasy_team_instance ti ON ti.id = side.instance_id
    CROSS JOIN LATERAL (
        VALUES
            ('bat1', ti.bat1),
            ('bat2', ti.bat2),
            ('wicket1', ti.wicket1),
            ('bowl1', ti.bowl1),
            ('bowl2', ti.bowl2),
            ('bowl3', ti.bowl3),
            ('all1', ti.all1),
            ('flex1', ti.flex1),
            ('bench1', ti.bench1),
            ('bench2', ti.bench2),
            ('bench3', ti.bench3)
    ) AS u(slot, player_id)
),

-- 9. Enrich with player info, match, and performance
enriched_players AS (
    SELECT
        rp.matchup_id,
        rp.league_id,
        rp.match_num,
        rp.team_side,
        rp.instance_id,
        rp.fantasy_team_id,
        rp.player_id,
        rp.slot,
        p.full_name AS player_name,
        p.image AS player_image,
        psi.role,
        t.name AS team_name,
        t.image AS team_image,
        t.abbreviation AS team_abbreviation,
        mi.id AS match_id,
        pp.id AS performance_id,
        psi.initial_projection
    FROM roster_players rp
    JOIN irldata.player p ON p.id = rp.player_id
    JOIN irldata.player_season_info psi
        ON psi.player_id = rp.player_id
       AND psi.season_id = rp.season_id
       AND psi.tournament_id = rp.tournament_id
    LEFT JOIN irldata.team t ON t.id = psi.team_id
    LEFT JOIN irldata.match_info mi
        ON mi.tournament_id = rp.tournament_id
       AND mi.season_id = rp.season_id
       AND (
            (mi.home_team_id = psi.team_id AND mi.home_match_num = rp.match_num)
            OR
            (mi.away_team_id = psi.team_id AND mi.away_match_num = rp.match_num)
       )
    LEFT JOIN irldata.player_performance pp
        ON pp.match_id = mi.id
       AND pp.player_season_id = psi.id
    WHERE rp.player_id IS NOT NULL
)

-- 10. Final output: matchups with player arrays per side
SELECT
    cm.league_id,
    cm.matchup_id,
    cm.match_num,
    cm.fantasy_team1_id,
    cm.fantasy_team_instance1_id,
    cm.captain1,
    cm.vice_captain1,
    cm.fantasy_team2_id,
    cm.fantasy_team_instance2_id,
    cm.captain2,
    cm.vice_captain2,
    COALESCE(
        (SELECT json_agg(json_build_object(
            'playerId', ep.player_id,
            'slot', ep.slot,
            'performanceId', ep.performance_id,
            'matchId', ep.match_id,
            'name', ep.player_name,
            'image', ep.player_image,
            'role', ep.role,
            'teamName', ep.team_name,
            'teamImage', ep.team_image,
            'teamAbbreviation', ep.team_abbreviation,
            'projectedPoints', ep.initial_projection
        ))
        FROM enriched_players ep
        WHERE ep.matchup_id = cm.matchup_id AND ep.team_side = 1),
        '[]'::json
    ) AS team1_players,
    COALESCE(
        (SELECT json_agg(json_build_object(
            'playerId', ep.player_id,
            'slot', ep.slot,
            'performanceId', ep.performance_id,
            'matchId', ep.match_id,
            'name', ep.player_name,
            'image', ep.player_image,
            'role', ep.role,
            'teamName', ep.team_name,
            'teamImage', ep.team_image,
            'teamAbbreviation', ep.team_abbreviation,
            'projectedPoints', ep.initial_projection
        ))
        FROM enriched_players ep
        WHERE ep.matchup_id = cm.matchup_id AND ep.team_side = 2),
        '[]'::json
    ) AS team2_players
FROM candidate_matchups cm
ORDER BY cm.league_id, cm.match_num;
    `;

    const result = await client.query(sql, [userId]);

    // Group by league
    const leagueMap: Record<string, { leagueId: string; matchups: any[] }> = {};
    for (const row of result.rows) {
      if (!leagueMap[row.league_id]) {
        leagueMap[row.league_id] = { leagueId: row.league_id, matchups: [] };
      }
      leagueMap[row.league_id].matchups.push({
        id: row.matchup_id,
        matchNum: row.match_num,
        team1: {
          fantasyTeamId: row.fantasy_team1_id,
          fantasyTeamInstanceId: row.fantasy_team_instance1_id,
          captain: row.captain1,
          viceCaptain: row.vice_captain1,
          players: row.team1_players,
        },
        team2: {
          fantasyTeamId: row.fantasy_team2_id,
          fantasyTeamInstanceId: row.fantasy_team_instance2_id,
          captain: row.captain2,
          viceCaptain: row.vice_captain2,
          players: row.team2_players,
        },
      });
    }

    return res.status(200).json(Object.values(leagueMap));

  } catch (err) {
    console.error("GET /matches/feed failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET SPECIFIC MATCH DETAILS
   GET /matches/:matchId
   ======================================================================================= */
app.get("/matchups/:matchUpId", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { matchUpId } = req.params;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!matchUpId) {
    return res.status(400).json({ message: "Matchup Id is required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      WITH
-- 1. Context: Matchup + League Details + Team IDs
match_ctx AS (
    SELECT
        m.id AS matchup_id,
        m.match_num,
        m.league_id,
        m.fantasy_team_instance1_id,
        m.fantasy_team_instance2_id,
        ti1.fantasy_team_id AS fantasy_team1_id,
        ti2.fantasy_team_id AS fantasy_team2_id,
        l.season_id,
        l.tournament_id
    FROM fantasydata.fantasy_matchups m
    JOIN fantasydata.leagues l ON l.id = m.league_id
    -- Join Instance 1 to get its Team ID
    JOIN fantasydata.fantasy_team_instance ti1 ON ti1.id = m.fantasy_team_instance1_id
    -- Join Instance 2 to get its Team ID
    JOIN fantasydata.fantasy_team_instance ti2 ON ti2.id = m.fantasy_team_instance2_id
    WHERE m.id = $1 -- Matchup UUID Input
),

-- 2. League Rules
league_rules AS (
    SELECT r.* FROM fantasydata.league_scoring_rules r
    JOIN match_ctx m ON m.league_id = r.league_id
),

-- 3. Unpivot Roster (Extracting PLAYER IDs)
team_rosters AS (
    -- Instance 1
    SELECT
        m.matchup_id,
        1 AS team_side,
        ti.id AS instance_id,
        ti.captain AS captain_player_id,
        ti.vice_captain AS vice_captain_player_id,
        UNNEST(ARRAY[
            bat1, bat2,
            bowl1, bowl2, bowl3,
            all1,
            wicket1,
            flex1
        ]) AS player_id
    FROM match_ctx m
    JOIN fantasydata.fantasy_team_instance ti ON ti.id = m.fantasy_team_instance1_id

    UNION ALL

    -- Instance 2
    SELECT
        m.matchup_id,
        2 AS team_side,
        ti.id AS instance_id,
        ti.captain AS captain_player_id,
        ti.vice_captain AS vice_captain_player_id,
        UNNEST(ARRAY[
            bat1, bat2,
            bowl1, bowl2, bowl3,
            all1,
            wicket1,
            flex1
        ]) AS player_id
    FROM match_ctx m
    JOIN fantasydata.fantasy_team_instance ti ON ti.id = m.fantasy_team_instance2_id
),

-- 4. The "Bridge": Link Player IDs to the Correct Performance
resolved_performances AS (
    SELECT
        tr.matchup_id,
        tr.team_side,
        tr.instance_id,
        tr.player_id,
        tr.captain_player_id,
        tr.vice_captain_player_id,
        pp.* -- Get all performance stats
    FROM team_rosters tr
    CROSS JOIN match_ctx mc

    -- A. Get Player Season Info
    JOIN irldata.player_season_info psi
        ON psi.player_id = tr.player_id
        AND psi.season_id = mc.season_id
        AND psi.tournament_id = mc.tournament_id

    -- B. Find the correct IRL Match Info
    JOIN irldata.match_info mi
        ON (
            (mi.home_team_id = psi.team_id AND mi.home_match_num = mc.match_num)
            OR
            (mi.away_team_id = psi.team_id AND mi.away_match_num = mc.match_num)
        )

    -- C. Get the Performance linked to that Match and Player Season
    JOIN irldata.player_performance pp
        ON pp.match_id = mi.id
        AND pp.player_season_id = psi.id

    WHERE tr.player_id IS NOT NULL
),

-- 5. Calculate Derived Stats
player_stats_calc AS (
    SELECT
        rp.*,
        CASE WHEN rp.balls_faced > 0 THEN (rp.runs_scored * 100.0 / rp.balls_faced)::NUMERIC ELSE 0 END AS strike_rate,
        CASE WHEN rp.balls_bowled > 0 THEN (rp.runs_conceded / (rp.balls_bowled / 6.0))::NUMERIC ELSE 0 END AS economy
    FROM resolved_performances rp
),

-- 6. Standard Scoring
standard_points AS (
    SELECT
        ps.player_id,
        ps.instance_id,
        SUM(
            CASE
                -- Batting
                WHEN r.stat = 'Points per run' THEN ps.runs_scored * r.per_unit_points
                WHEN r.stat = 'Bonus per 4' THEN ps.fours * r.per_unit_points
                WHEN r.stat = 'Bonus per 6' THEN ps.sixes * r.per_unit_points
                WHEN r.stat = 'Bonus per half-century' AND ps.runs_scored >= 50 THEN r.flat_points
                WHEN r.stat = 'Bonus per century' AND ps.runs_scored >= 100 THEN r.flat_points
                WHEN r.stat = 'Duck-out Penalty' AND ps.runs_scored = 0 AND ps.balls_faced > 0 THEN r.flat_points

                -- Bowling
                WHEN r.stat = 'Points per Wicket' THEN ps.wickets_taken * r.per_unit_points
                WHEN r.stat = '3-Wicket Bonus' THEN FLOOR(ps.wickets_taken / 3.0) * r.per_unit_points
                WHEN r.stat = '5-Wicket Bonus' THEN FLOOR(ps.wickets_taken / 5.0) * r.per_unit_points

                -- Fielding
                WHEN r.stat = 'Points per catch' THEN ps.catches * r.per_unit_points
                WHEN r.stat = '3-Catches bonus' THEN FLOOR(ps.catches / 3.0) * r.per_unit_points
                WHEN r.stat = 'Run Out' THEN ps.run_outs * r.per_unit_points
                WHEN r.stat = 'Dropped Catch' THEN ps.catches_dropped * r.per_unit_points
                ELSE 0
            END
        ) AS total_std_points
    FROM player_stats_calc ps
    CROSS JOIN league_rules r
    WHERE r.mode != 'band' AND r.category != 'leadership'
    GROUP BY ps.player_id, ps.instance_id
),

-- 7. Band Scoring
band_points AS (
    SELECT
        ps.player_id,
        ps.instance_id,
        SUM(r.flat_points) AS total_band_points
    FROM player_stats_calc ps
    JOIN league_rules r ON r.mode = 'band'
    WHERE
        (r.stat = 'Strike Rate' AND ps.balls_faced > 0 AND r.band @> ps.strike_rate)
        OR
        (r.stat = 'Economy' AND ps.balls_bowled > 0 AND r.band @> ps.economy)
    GROUP BY ps.player_id, ps.instance_id
),

-- 8. Final Calculation per Player
individual_scores AS (
    SELECT
        ps.matchup_id,
        ps.team_side,
        ps.instance_id,
        ps.player_id,

        (COALESCE(sp.total_std_points, 0) + COALESCE(bp.total_band_points, 0))

        * COALESCE((
            SELECT multiplier FROM league_rules
            WHERE stat = 'Captaincy Multiplier' AND ps.player_id = ps.captain_player_id
        ), 1)

        * COALESCE((
            SELECT multiplier FROM league_rules
            WHERE stat = 'Vice Captaincy Multiplier' AND ps.player_id = ps.vice_captain_player_id
        ), 1) AS final_player_score

    FROM player_stats_calc ps
    LEFT JOIN standard_points sp ON sp.player_id = ps.player_id AND sp.instance_id = ps.instance_id
    LEFT JOIN band_points bp ON bp.player_id = ps.player_id AND bp.instance_id = ps.instance_id
)

-- 9. Final Output Aggregation
SELECT
    m.matchup_id AS id,
    m.league_id,
    m.match_num,
    m.fantasy_team_instance1_id,
    m.fantasy_team_instance2_id,
    m.fantasy_team1_id,
    m.fantasy_team2_id,
    COALESCE(SUM(CASE WHEN ind.team_side = 1 THEN ind.final_player_score ELSE 0 END), 0) AS fantasy_team_instance1_score,
    COALESCE(SUM(CASE WHEN ind.team_side = 2 THEN ind.final_player_score ELSE 0 END), 0) AS fantasy_team_instance2_score
FROM match_ctx m
LEFT JOIN individual_scores ind ON ind.matchup_id = m.matchup_id
GROUP BY
    m.matchup_id,
    m.league_id,
    m.match_num,
    m.fantasy_team_instance1_id,
    m.fantasy_team_instance2_id,
    m.fantasy_team1_id,
    m.fantasy_team2_id;
    `;

    const result = await client.query(sql, [matchUpId]);
    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /matches/:matchupId failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET ALL MATCHUPS FOR A LEAGUE AT A SPECIFIC MATCH NUM
   GET /matchups/league/:leagueId/week/:matchNum
   ======================================================================================= */
app.get("/matchups/league/:leagueId/week/:matchNum", async (req: Request, res: Response) => {
  const { leagueId, matchNum } = req.params;

  if (!leagueId || !matchNum) {
    return res.status(400).json({ message: "leagueId and matchNum are required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
WITH
league_ctx AS (
    SELECT l.id AS league_id, l.season_id, l.tournament_id
    FROM fantasydata.leagues l
    WHERE l.id = $1
),

target_matchups AS (
    SELECT
        m.id AS matchup_id, m.match_num, m.league_id,
        m.fantasy_team_instance1_id, m.fantasy_team_instance2_id,
        ti1.fantasy_team_id AS fantasy_team1_id,
        ti2.fantasy_team_id AS fantasy_team2_id,
        ti1.captain AS captain1, ti1.vice_captain AS vice_captain1,
        ti2.captain AS captain2, ti2.vice_captain AS vice_captain2,
        lc.season_id, lc.tournament_id
    FROM fantasydata.fantasy_matchups m
    JOIN league_ctx lc ON lc.league_id = m.league_id
    JOIN fantasydata.fantasy_team_instance ti1 ON ti1.id = m.fantasy_team_instance1_id
    JOIN fantasydata.fantasy_team_instance ti2 ON ti2.id = m.fantasy_team_instance2_id
    WHERE m.league_id = $1 AND m.match_num = $2
),

-- Distinct IRL teams from all rosters to check match statuses
roster_irl_teams AS (
    SELECT DISTINCT psi.team_id, tm.season_id, tm.tournament_id
    FROM target_matchups tm
    JOIN fantasydata.fantasy_team_instance ti
        ON ti.id IN (tm.fantasy_team_instance1_id, tm.fantasy_team_instance2_id)
    CROSS JOIN LATERAL (VALUES
        (ti.bat1),(ti.bat2),(ti.wicket1),
        (ti.bowl1),(ti.bowl2),(ti.bowl3),
        (ti.all1),(ti.flex1)
    ) AS u(player_id)
    JOIN irldata.player_season_info psi
        ON psi.player_id = u.player_id
       AND psi.season_id = tm.season_id
       AND psi.tournament_id = tm.tournament_id
    WHERE u.player_id IS NOT NULL
),

-- Determine matchnum status using the same definition as the feed
matchnum_status AS (
    SELECT CASE
        WHEN COUNT(*) = 0 THEN 'upcoming'
        WHEN COUNT(*) FILTER (WHERE mi.status = 'LIVE') > 0 THEN 'active'
        WHEN COUNT(*) FILTER (WHERE mi.status IN ('FINISHED','ABAN.')) > 0
             AND COUNT(*) FILTER (WHERE mi.status IN ('NS','LIVE')) > 0 THEN 'active'
        WHEN COUNT(*) FILTER (WHERE mi.status = 'NS') = COUNT(*) THEN 'upcoming'
        ELSE 'completed'
    END AS status
    FROM roster_irl_teams rt
    JOIN irldata.match_info mi
        ON mi.tournament_id = rt.tournament_id
       AND mi.season_id = rt.season_id
       AND (
            (mi.home_team_id = rt.team_id AND mi.home_match_num = $2)
            OR (mi.away_team_id = rt.team_id AND mi.away_match_num = $2)
       )
),

league_rules AS (
    SELECT r.* FROM fantasydata.league_scoring_rules r WHERE r.league_id = $1
),

-- Unpivot rosters for both sides of every matchup
roster_players AS (
    SELECT
        tm.matchup_id, tm.match_num, tm.season_id, tm.tournament_id,
        side.team_side, side.instance_id, side.fantasy_team_id,
        side.captain, side.vice_captain,
        u.slot, u.player_id
    FROM target_matchups tm
    CROSS JOIN LATERAL (VALUES
        (1, tm.fantasy_team_instance1_id, tm.fantasy_team1_id, tm.captain1, tm.vice_captain1),
        (2, tm.fantasy_team_instance2_id, tm.fantasy_team2_id, tm.captain2, tm.vice_captain2)
    ) AS side(team_side, instance_id, fantasy_team_id, captain, vice_captain)
    JOIN fantasydata.fantasy_team_instance ti ON ti.id = side.instance_id
    CROSS JOIN LATERAL (VALUES
        ('bat1', ti.bat1), ('bat2', ti.bat2), ('wicket1', ti.wicket1),
        ('bowl1', ti.bowl1), ('bowl2', ti.bowl2), ('bowl3', ti.bowl3),
        ('all1', ti.all1), ('flex1', ti.flex1),
        ('bench1', ti.bench1), ('bench2', ti.bench2), ('bench3', ti.bench3)
    ) AS u(slot, player_id)
),

-- Enrich with player info + performance
enriched_players AS (
    SELECT
        rp.matchup_id, rp.team_side, rp.instance_id, rp.fantasy_team_id,
        rp.player_id, rp.slot, rp.captain, rp.vice_captain,
        p.full_name AS player_name, p.image AS player_image,
        psi.role, psi.initial_projection,
        t.name AS team_name, t.image AS team_image, t.abbreviation AS team_abbreviation,
        mi.id AS match_id,
        pp.runs_scored, pp.balls_faced, pp.fours, pp.sixes,
        pp.balls_bowled, pp.runs_conceded, pp.wickets_taken,
        pp.catches, pp.run_outs, pp.catches_dropped, pp.not_out
    FROM roster_players rp
    LEFT JOIN irldata.player p ON p.id = rp.player_id
    LEFT JOIN irldata.player_season_info psi
        ON psi.player_id = rp.player_id
       AND psi.season_id = rp.season_id
       AND psi.tournament_id = rp.tournament_id
    LEFT JOIN irldata.team t ON t.id = psi.team_id
    LEFT JOIN irldata.match_info mi
        ON mi.tournament_id = rp.tournament_id
       AND mi.season_id = rp.season_id
       AND (
            (mi.home_team_id = psi.team_id AND mi.home_match_num = rp.match_num)
            OR (mi.away_team_id = psi.team_id AND mi.away_match_num = rp.match_num)
       )
    LEFT JOIN irldata.player_performance pp
        ON pp.match_id = mi.id
       AND pp.player_season_id = psi.id
    WHERE rp.player_id IS NOT NULL
),

-- Derived stats for scoring
player_stats_calc AS (
    SELECT ep.*,
        CASE WHEN COALESCE(ep.balls_faced, 0) > 0
             THEN (ep.runs_scored * 100.0 / ep.balls_faced)::NUMERIC ELSE 0 END AS strike_rate,
        CASE WHEN COALESCE(ep.balls_bowled, 0) > 0
             THEN (ep.runs_conceded / (ep.balls_bowled / 6.0))::NUMERIC ELSE 0 END AS economy
    FROM enriched_players ep
),

-- Standard scoring rules
standard_points AS (
    SELECT ps.player_id, ps.instance_id,
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
    CROSS JOIN league_rules r
    WHERE r.mode != 'band' AND r.category != 'leadership'
    GROUP BY ps.player_id, ps.instance_id
),

-- Band scoring (strike rate / economy brackets)
band_points AS (
    SELECT ps.player_id, ps.instance_id,
        SUM(r.flat_points) AS total_band_points
    FROM player_stats_calc ps
    JOIN league_rules r ON r.mode = 'band'
    WHERE
        (r.stat = 'Strike Rate' AND COALESCE(ps.balls_faced, 0) > 0 AND r.band @> ps.strike_rate)
        OR
        (r.stat = 'Economy' AND COALESCE(ps.balls_bowled, 0) > 0 AND r.band @> ps.economy)
    GROUP BY ps.player_id, ps.instance_id
),

-- Per-player fantasy scores with captain/vc multipliers
individual_scores AS (
    SELECT
        ep.matchup_id, ep.team_side, ep.instance_id, ep.player_id,
        (COALESCE(sp.total_std_points, 0) + COALESCE(bp.total_band_points, 0))
        * COALESCE((
            SELECT multiplier FROM league_rules
            WHERE stat = 'Captaincy Multiplier' AND ep.player_id = ep.captain
        ), 1)
        * COALESCE((
            SELECT multiplier FROM league_rules
            WHERE stat = 'Vice Captaincy Multiplier' AND ep.player_id = ep.vice_captain
        ), 1) AS fantasy_points
    FROM enriched_players ep
    LEFT JOIN standard_points sp ON sp.player_id = ep.player_id AND sp.instance_id = ep.instance_id
    LEFT JOIN band_points bp ON bp.player_id = ep.player_id AND bp.instance_id = ep.instance_id
)

-- Final output
SELECT
    (SELECT status FROM matchnum_status) AS matchnum_status,
    tm.matchup_id, tm.match_num, tm.league_id,
    tm.fantasy_team1_id, tm.fantasy_team_instance1_id, tm.captain1, tm.vice_captain1,
    tm.fantasy_team2_id, tm.fantasy_team_instance2_id, tm.captain2, tm.vice_captain2,

    COALESCE(
        (SELECT json_agg(json_build_object(
            'playerId', ep.player_id,
            'slot', ep.slot,
            'name', ep.player_name,
            'image', ep.player_image,
            'role', ep.role,
            'teamName', ep.team_name,
            'teamImage', ep.team_image,
            'teamAbbreviation', ep.team_abbreviation,
            'matchId', ep.match_id,
            'projectedPoints', ep.initial_projection,
            'fantasyPoints', CASE WHEN (SELECT status FROM matchnum_status) = 'upcoming'
                THEN NULL ELSE ind.fantasy_points END,
            'performance', CASE WHEN (SELECT status FROM matchnum_status) = 'upcoming'
                THEN NULL
                ELSE json_build_object(
                    'runs_scored', ep.runs_scored,
                    'balls_faced', ep.balls_faced,
                    'fours', ep.fours,
                    'sixes', ep.sixes,
                    'balls_bowled', ep.balls_bowled,
                    'runs_conceded', ep.runs_conceded,
                    'wickets_taken', ep.wickets_taken,
                    'catches', ep.catches,
                    'run_outs', ep.run_outs,
                    'catches_dropped', ep.catches_dropped,
                    'not_out', ep.not_out
                )
            END
        ))
        FROM enriched_players ep
        LEFT JOIN individual_scores ind
            ON ind.player_id = ep.player_id
           AND ind.instance_id = ep.instance_id
           AND ind.matchup_id = ep.matchup_id
        WHERE ep.matchup_id = tm.matchup_id AND ep.team_side = 1),
        '[]'::json
    ) AS team1_players,

    COALESCE(
        (SELECT json_agg(json_build_object(
            'playerId', ep.player_id,
            'slot', ep.slot,
            'name', ep.player_name,
            'image', ep.player_image,
            'role', ep.role,
            'teamName', ep.team_name,
            'teamImage', ep.team_image,
            'teamAbbreviation', ep.team_abbreviation,
            'matchId', ep.match_id,
            'projectedPoints', ep.initial_projection,
            'fantasyPoints', CASE WHEN (SELECT status FROM matchnum_status) = 'upcoming'
                THEN NULL ELSE ind.fantasy_points END,
            'performance', CASE WHEN (SELECT status FROM matchnum_status) = 'upcoming'
                THEN NULL
                ELSE json_build_object(
                    'runs_scored', ep.runs_scored,
                    'balls_faced', ep.balls_faced,
                    'fours', ep.fours,
                    'sixes', ep.sixes,
                    'balls_bowled', ep.balls_bowled,
                    'runs_conceded', ep.runs_conceded,
                    'wickets_taken', ep.wickets_taken,
                    'catches', ep.catches,
                    'run_outs', ep.run_outs,
                    'catches_dropped', ep.catches_dropped,
                    'not_out', ep.not_out
                )
            END
        ))
        FROM enriched_players ep
        LEFT JOIN individual_scores ind
            ON ind.player_id = ep.player_id
           AND ind.instance_id = ep.instance_id
           AND ind.matchup_id = ep.matchup_id
        WHERE ep.matchup_id = tm.matchup_id AND ep.team_side = 2),
        '[]'::json
    ) AS team2_players

FROM target_matchups tm
ORDER BY tm.matchup_id;
    `;

    const result = await client.query(sql, [leagueId, parseInt(matchNum)]);

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "No matchups found for this league and match number" });
    }

    const status = result.rows[0].matchnum_status;
    const matchups = result.rows.map((row: any) => ({
      id: row.matchup_id,
      matchNum: row.match_num,
      team1: {
        fantasyTeamId: row.fantasy_team1_id,
        fantasyTeamInstanceId: row.fantasy_team_instance1_id,
        captain: row.captain1,
        viceCaptain: row.vice_captain1,
        players: row.team1_players,
      },
      team2: {
        fantasyTeamId: row.fantasy_team2_id,
        fantasyTeamInstanceId: row.fantasy_team_instance2_id,
        captain: row.captain2,
        viceCaptain: row.vice_captain2,
        players: row.team2_players,
      },
    }));

    return res.json({
      leagueId,
      matchNum: parseInt(matchNum),
      status,
      matchups,
    });

  } catch (err) {
    console.error("GET /matchups/league/:leagueId/week/:matchNum failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
