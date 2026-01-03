import { APIGatewayProxyEvent, Context } from "aws-lambda";
import pg from "pg";
import fs from "fs";
import express from "express";
import serverless from "serverless-http";
import cors from "cors";

/**
 * Extend Express Request to include Lambda event/context
 */
declare global {
  namespace Express {
    interface Request {
      lambdaEvent: APIGatewayProxyEvent;
      lambdaContext: Context;
    }
  }
}

const getPool = (): pg.Pool => {
  const rdsCa = fs.readFileSync("/opt/nodejs/us-west-2-bundle.pem").toString();

  return new pg.Pool({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASSWORD,
    database: process.env.DB_NAME,
    max: 1,
    idleTimeoutMillis: 5000,
    connectionTimeoutMillis: 5000,
    ssl: {
      rejectUnauthorized: true,
      ca: rdsCa
    }
  });
};

const app = express();
app.use(cors());
app.use(express.json());

/* =======================================================================================
   GET ALL LIVE / UPCOMING MATCHES FOR USER (HOME FEED)
   GET /matches/feed
   ======================================================================================= */
app.get("/matchups/feed", async (req, res) => {
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
-- 1. User Teams
user_teams AS (
    SELECT id AS team_id 
    FROM fantasydata.fantasy_teams 
    WHERE user_id = $1
),

-- 2. Matchups involving user
target_matchup_ids AS (
    SELECT m.id AS matchup_id
    FROM fantasydata.fantasy_matchups m
    JOIN fantasydata.fantasy_team_instance ti1 ON ti1.id = m.fantasy_team_instance1_id
    JOIN fantasydata.fantasy_team_instance ti2 ON ti2.id = m.fantasy_team_instance2_id
    WHERE ti1.fantasy_team_id IN (SELECT team_id FROM user_teams)
       OR ti2.fantasy_team_id IN (SELECT team_id FROM user_teams)
    GROUP BY m.id
),

-- 3. Matchup context
candidate_matchups AS (
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
    JOIN target_matchup_ids tm ON tm.matchup_id = m.id
    JOIN fantasydata.leagues l ON l.id = m.league_id
    JOIN fantasydata.fantasy_team_instance ti1 ON ti1.id = m.fantasy_team_instance1_id
    JOIN fantasydata.fantasy_team_instance ti2 ON ti2.id = m.fantasy_team_instance2_id
),

-- 4. Unpivot team rosters
team_rosters AS (
    SELECT 
        cm.matchup_id,
        cm.league_id,
        1 AS team_side,
        cm.fantasy_team_instance1_id AS instance_id,
        ti.captain AS captain_player_id,
        ti.vice_captain AS vice_captain_player_id,
        UNNEST(ARRAY[
            bat1, bat2, bat3, bat4,
            bowl1, bowl2, bowl3, bowl4,
            all1, all2, all3,
            wicket1, wicket2,
            flex1, flex2
        ]) AS player_id
    FROM candidate_matchups cm
    JOIN fantasydata.fantasy_team_instance ti ON ti.id = cm.fantasy_team_instance1_id

    UNION ALL

    SELECT 
        cm.matchup_id,
        cm.league_id,
        2 AS team_side,
        cm.fantasy_team_instance2_id AS instance_id,
        ti.captain AS captain_player_id,
        ti.vice_captain AS vice_captain_player_id,
        UNNEST(ARRAY[
            bat1, bat2, bat3, bat4,
            bowl1, bowl2, bowl3, bowl4,
            all1, all2, all3,
            wicket1, wicket2,
            flex1, flex2
        ]) AS player_id
    FROM candidate_matchups cm
    JOIN fantasydata.fantasy_team_instance ti ON ti.id = cm.fantasy_team_instance2_id
),

-- 5. Resolve real-world match & player season
roster_match_status AS (
    SELECT DISTINCT
        tr.matchup_id,
        tr.player_id,
        mi.id AS real_match_id,
        mi.status AS match_status,
        psi.id AS player_season_id
    FROM team_rosters tr
    JOIN candidate_matchups cm ON cm.matchup_id = tr.matchup_id
    JOIN irldata.player_season_info psi
        ON psi.player_id = tr.player_id
       AND psi.season_id = cm.season_id
       AND psi.tournament_id = cm.tournament_id
    JOIN irldata.match_info mi
        ON (
            (mi.home_team_id = psi.team_id AND mi.home_match_num = cm.match_num)
            OR
            (mi.away_team_id = psi.team_id AND mi.away_match_num = cm.match_num)
        )
    WHERE tr.player_id IS NOT NULL
),

-- 6. Valid matchups
valid_matchups AS (
    SELECT matchup_id
    FROM roster_match_status
    GROUP BY matchup_id
    HAVING 
        COUNT(*) FILTER (WHERE match_status = 'LIVE') > 0
        OR (
            COUNT(*) FILTER (WHERE match_status IN ('FINISHED','ABAN.')) > 0
            AND COUNT(*) FILTER (WHERE match_status = 'NS') > 0
        )
),

-- 7. League rules
league_rules AS (
    SELECT *
    FROM fantasydata.league_scoring_rules
    WHERE league_id IN (SELECT DISTINCT league_id FROM candidate_matchups)
),

-- 8. Raw performances (can duplicate)
resolved_performances AS (
    SELECT 
        tr.matchup_id,
        tr.league_id,
        tr.team_side,
        tr.instance_id,
        tr.player_id,
        tr.captain_player_id,
        tr.vice_captain_player_id,
        pp.*
    FROM team_rosters tr
    JOIN valid_matchups vm ON vm.matchup_id = tr.matchup_id
    JOIN roster_match_status rms
        ON rms.matchup_id = tr.matchup_id
       AND rms.player_id = tr.player_id
    LEFT JOIN irldata.player_performance pp
        ON pp.match_id = rms.real_match_id
       AND pp.player_season_id = rms.player_season_id
),

-- 🔥 9. HARD AGGREGATION BARRIER (THE FIX)
player_matchup_stats AS (
    SELECT
        matchup_id,
        league_id,
        team_side,
        instance_id,
        player_id,
        captain_player_id,
        vice_captain_player_id,

        SUM(COALESCE(runs_scored, 0))       AS runs_scored,
        SUM(COALESCE(balls_faced, 0))       AS balls_faced,
        SUM(COALESCE(fours, 0))             AS fours,
        SUM(COALESCE(sixes, 0))             AS sixes,
        SUM(COALESCE(balls_bowled, 0))      AS balls_bowled,
        SUM(COALESCE(runs_conceded, 0))     AS runs_conceded,
        SUM(COALESCE(wickets_taken, 0))     AS wickets_taken,
        SUM(COALESCE(catches, 0))           AS catches,
        SUM(COALESCE(run_outs, 0))           AS run_outs,
        SUM(COALESCE(catches_dropped, 0))   AS catches_dropped

    FROM resolved_performances
    GROUP BY
        matchup_id,
        league_id,
        team_side,
        instance_id,
        player_id,
        captain_player_id,
        vice_captain_player_id
),

-- 10. Derived stats
player_stats_calc AS (
    SELECT
        pms.*,
        CASE WHEN balls_faced > 0
             THEN (runs_scored * 100.0 / balls_faced)::NUMERIC
             ELSE 0 END AS strike_rate,
        CASE WHEN balls_bowled > 0
             THEN (runs_conceded / (balls_bowled / 6.0))::NUMERIC
             ELSE 0 END AS economy
    FROM player_matchup_stats pms
),

-- 11. Standard points
standard_points AS (
    SELECT
        matchup_id,
        player_id,
        instance_id,
        SUM(
            CASE
                WHEN r.stat = 'Points per run' THEN runs_scored * r.per_unit_points
                WHEN r.stat = 'Bonus per 4' THEN fours * r.per_unit_points
                WHEN r.stat = 'Bonus per 6' THEN sixes * r.per_unit_points
                WHEN r.stat = 'Bonus per half-century' AND runs_scored >= 50 THEN r.flat_points
                WHEN r.stat = 'Bonus per century' AND runs_scored >= 100 THEN r.flat_points
                WHEN r.stat = 'Duck-out Penalty' AND runs_scored = 0 AND balls_faced > 0 THEN r.flat_points
                WHEN r.stat = 'Points per Wicket' THEN wickets_taken * r.per_unit_points
                WHEN r.stat = '3-Wicket Bonus' THEN FLOOR(wickets_taken / 3.0) * r.per_unit_points
                WHEN r.stat = '5-Wicket Bonus' THEN FLOOR(wickets_taken / 5.0) * r.per_unit_points
                WHEN r.stat = 'Points per catch' THEN catches * r.per_unit_points
                WHEN r.stat = '3-Catches bonus' THEN FLOOR(catches / 3.0) * r.per_unit_points
                WHEN r.stat = 'Run Out' THEN run_outs * r.per_unit_points
                WHEN r.stat = 'Dropped Catch' THEN catches_dropped * r.per_unit_points
                ELSE 0
            END
        ) AS total_std_points
    FROM player_stats_calc ps
    JOIN league_rules r ON r.league_id = ps.league_id
    WHERE r.mode != 'band' AND r.category != 'leadership'
    GROUP BY matchup_id, player_id, instance_id
),

-- 12. Band points
band_points AS (
    SELECT
        matchup_id,
        player_id,
        instance_id,
        SUM(r.flat_points) AS total_band_points
    FROM player_stats_calc ps
    JOIN league_rules r
        ON r.league_id = ps.league_id
       AND r.mode = 'band'
    WHERE
        (r.stat = 'Strike Rate' AND balls_faced > 0 AND r.band @> strike_rate)
        OR
        (r.stat = 'Economy' AND balls_bowled > 0 AND r.band @> economy)
    GROUP BY matchup_id, player_id, instance_id
),

-- 13. Individual scores
individual_scores AS (
    SELECT
        ps.matchup_id,
        ps.team_side,
        ps.instance_id,
        ps.player_id,

        (COALESCE(sp.total_std_points,0) + COALESCE(bp.total_band_points,0))
        * CASE WHEN ps.player_id = ps.captain_player_id
               THEN (SELECT multiplier FROM league_rules WHERE stat='Captaincy Multiplier' AND league_id=ps.league_id LIMIT 1)
               ELSE 1 END
        * CASE WHEN ps.player_id = ps.vice_captain_player_id
               THEN (SELECT multiplier FROM league_rules WHERE stat='Vice Captaincy Multiplier' AND league_id=ps.league_id LIMIT 1)
               ELSE 1 END
        AS final_player_score
    FROM player_stats_calc ps
    LEFT JOIN standard_points sp USING (matchup_id, player_id, instance_id)
    LEFT JOIN band_points bp USING (matchup_id, player_id, instance_id)
),

-- 14. Matchup totals
matchup_totals AS (
    SELECT
        matchup_id,
        SUM(CASE WHEN team_side = 1 THEN final_player_score ELSE 0 END)::FLOAT AS score1,
        SUM(CASE WHEN team_side = 2 THEN final_player_score ELSE 0 END)::FLOAT AS score2
    FROM individual_scores
    GROUP BY matchup_id
)

-- 15. Final output
SELECT
    cm.matchup_id AS id,
    cm.league_id,
    cm.match_num,
    cm.fantasy_team_instance1_id,
    cm.fantasy_team_instance2_id,
    cm.fantasy_team1_id,
    cm.fantasy_team2_id,
    COALESCE(mt.score1,0) AS fantasy_team_instance1_score,
    COALESCE(mt.score2,0) AS fantasy_team_instance2_score
FROM candidate_matchups cm
JOIN valid_matchups vm ON vm.matchup_id = cm.matchup_id
LEFT JOIN matchup_totals mt ON mt.matchup_id = cm.matchup_id
ORDER BY cm.matchup_id;
    `;

    const result = await client.query(sql, [userId]);

    return res.status(200).json(result.rows);

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
app.get("/matchups/:matchUpId", async (req, res) => {
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
            bat1, bat2, bat3, bat4, 
            bowl1, bowl2, bowl3, bowl4, 
            all1, all2, all3, 
            wicket1, wicket2, 
            flex1, flex2
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
            bat1, bat2, bat3, bat4, 
            bowl1, bowl2, bowl3, bowl4, 
            all1, all2, all3, 
            wicket1, wicket2, 
            flex1, flex2
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
   EXPORT LAMBDA HANDLER
   ======================================================================================= */
export const lambdaHandler = serverless(app, {
  request: (req: any, event: APIGatewayProxyEvent, context: Context) => {
    req.lambdaEvent = event;
    req.lambdaContext = context;
  }
});