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
      ca: rdsCa,
    },
  });
};

const app = express();
app.use(cors());
app.use(express.json());

/* =======================================================================================
   GET MATCHUPS WITH CALCULATED SCORES
   GET /matchups?match_num=#
   ======================================================================================= */
app.get("/matchups", async (req, res) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  const matchNumStr = req.query.match_num as string | undefined;

  if (!matchNumStr) {
    return res.status(400).json({
      message: "match_num query parameter is required",
    });
  }

  const matchNum = Number(matchNumStr);
  if (Number.isNaN(matchNum)) {
    return res.status(400).json({
      message: "match_num must be a valid number",
    });
  }

  let client: pg.PoolClient | null = null;

  try {
    client = await getPool().connect();

    const result = await client.query(
      `
WITH fti AS (
    SELECT 
        fti.id,
        fti.fantasy_team_id,
        fti.match_num,
        ARRAY[
            bat1, bat2, bat3, bat4,
            bowl1, bowl2, bowl3, bowl4,
            all1, all2, all3,
            wicket1, wicket2,
            flex1, flex2, flex3, flex4
        ] AS player_ids
    FROM fantasydata.fantasy_team_instance fti
),

team_info AS (
    SELECT 
        fti.id AS fti_id,
        ft.id AS fantasy_team_id,
        ft.user_id,
        l.id AS league_id,
        l.season_id,
        fti.match_num,
        fti.player_ids
    FROM fti
    JOIN fantasydata.fantasy_teams ft
        ON ft.id = fti.fantasy_team_id
    JOIN fantasydata.leagues l
        ON l.id = ft.league_id
),

player_seasons AS (
    SELECT DISTINCT
        psi.id AS player_season_id,
        psi.team_id,
        psi.season_id,
        tinfo.match_num,
        tinfo.fti_id
    FROM irldata.player_season_info psi
    JOIN team_info tinfo
        ON psi.season_id = tinfo.season_id
    WHERE psi.player_id = ANY(tinfo.player_ids)
),

match_lookup AS (
    SELECT 
        mi.id AS match_id,
        mi.season_id,
        mi.home_team_id,
        mi.away_team_id,
        mi.home_match_num,
        mi.away_match_num
    FROM irldata.match_info mi
),

scoring AS (
    SELECT 
        ps.fti_id,
        SUM(
            -- FIELDING
            (COALESCE(ppa.catches, 0) * 8) +
            (CASE WHEN COALESCE(ppa.catches, 0) >= 3 THEN 4 ELSE 0 END) +

            -- BATTING
            COALESCE(ppa.runs_scored, 0) +
            COALESCE(ppa.fours, 0) +
            (COALESCE(ppa.sixes, 0) * 2) +
            (CASE WHEN COALESCE(ppa.runs_scored, 0) > 50 THEN 8 ELSE 0 END) +
            (CASE WHEN COALESCE(ppa.runs_scored, 0) > 100 THEN 8 ELSE 0 END) +

            -- STRIKE RATE
            (CASE 
                WHEN COALESCE(ppa.balls_faced, 0) = 0 THEN 0
                ELSE CASE
                    WHEN (ppa.runs_scored * 100.0 / ppa.balls_faced) BETWEEN 0 AND 30 THEN -6
                    WHEN (ppa.runs_scored * 100.0 / ppa.balls_faced) <= 39 THEN -4
                    WHEN (ppa.runs_scored * 100.0 / ppa.balls_faced) <= 50 THEN -2
                    WHEN (ppa.runs_scored * 100.0 / ppa.balls_faced) BETWEEN 100 AND 119 THEN 2
                    WHEN (ppa.runs_scored * 100.0 / ppa.balls_faced) BETWEEN 120 AND 139 THEN 4
                    WHEN (ppa.runs_scored * 100.0 / ppa.balls_faced) >= 140 THEN 6
                    ELSE 0
                END
            END) +

            -- BOWLING
            (COALESCE(ppa.wickets_taken, 0) * 25) +
            (CASE WHEN COALESCE(ppa.wickets_taken, 0) > 3 THEN 4 ELSE 0 END) +
            (CASE WHEN COALESCE(ppa.wickets_taken, 0) > 5 THEN 5 ELSE 0 END) +

            -- ECONOMY
            (CASE
                WHEN COALESCE(ppa.balls_bowled, 0) = 0 THEN 0
                ELSE CASE
                    WHEN (ppa.runs_conceded * 6.0 / ppa.balls_bowled) <= 2.5 THEN 6
                    WHEN (ppa.runs_conceded * 6.0 / ppa.balls_bowled) <= 3.49 THEN 4
                    WHEN (ppa.runs_conceded * 6.0 / ppa.balls_bowled) <= 4.5 THEN 2
                    WHEN (ppa.runs_conceded * 6.0 / ppa.balls_bowled) BETWEEN 7 AND 8 THEN -2
                    WHEN (ppa.runs_conceded * 6.0 / ppa.balls_bowled) <= 9 THEN -4
                    WHEN (ppa.runs_conceded * 6.0 / ppa.balls_bowled) > 9 THEN -6
                    ELSE 0
                END
            END)
        ) AS total_points
    FROM player_seasons ps
    JOIN match_lookup ml
        ON ml.season_id = ps.season_id
       AND (
            (ps.team_id = ml.home_team_id AND ml.home_match_num = ps.match_num)
         OR (ps.team_id = ml.away_team_id AND ml.away_match_num = ps.match_num)
       )
    LEFT JOIN irldata.player_performance ppa
        ON ppa.player_season_id = ps.player_season_id
       AND ppa.match_id = ml.match_id
    GROUP BY ps.fti_id
)

SELECT
    fm.id AS id,
    fm.league_id,
    fm.match_num,

    -- Team 1
    fti1.id AS fantasy_team_instance1_id,
    ft1.id AS fantasy_team1_id,
    ft1.team_name AS fantasy_team1_name,
    COALESCE(s1.total_points, 0) AS fantasy_team_instance1_score,

    -- Team 2
    fti2.id AS fantasy_team_instance2_id,
    ft2.id AS fantasy_team2_id,
    ft2.team_name AS fantasy_team2_name,
    COALESCE(s2.total_points, 0) AS fantasy_team_instance2_score

FROM fantasydata.fantasy_matchups fm
JOIN fantasydata.fantasy_team_instance fti1
    ON fti1.id = fm.fantasy_team_instance1_id
JOIN fantasydata.fantasy_team_instance fti2
    ON fti2.id = fm.fantasy_team_instance2_id
JOIN fantasydata.fantasy_teams ft1
    ON ft1.id = fti1.fantasy_team_id
JOIN fantasydata.fantasy_teams ft2
    ON ft2.id = fti2.fantasy_team_id
LEFT JOIN scoring s1 ON s1.fti_id = fti1.id
LEFT JOIN scoring s2 ON s2.fti_id = fti2.id
WHERE
    fm.match_num = $2
    AND (ft1.user_id = $1 OR ft2.user_id = $1)
ORDER BY fm.match_num DESC;
      `,
      [userId, matchNum]
    );

    return res.status(200).json(result.rows);
  } catch (err) {
    console.error("GET /matchups failed:", err);
    return res.status(500).json({ message: "Internal server error" });
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
  },
});