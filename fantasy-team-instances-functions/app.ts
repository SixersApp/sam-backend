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
   GET FANTASY TEAM INSTANCE (teamId + matchNum)
   GET /fantasy-team-instance?teamId=&matchNum=
   ======================================================================================= */
app.get("/fantasy-team-instance", async (req, res) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  const { fantasyTeamId, matchNum } = req.query;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!fantasyTeamId || !matchNum) {
    return res
      .status(400)
      .json({ message: "Missing fantasyTeamId or matchNum" });
  }

  const parsedMatchNum = Number(matchNum);

  if (Number.isNaN(parsedMatchNum)) {
    return res.status(400).json({ message: "matchNum must be a number" });
  }

  let client: pg.PoolClient | null = null;

  try {
    client = await getPool().connect();

    // ---------- Ownership check ----------
    const ownerCheck = await client.query(
      `
      SELECT 1
      FROM fantasydata.fantasy_teams
      WHERE id = $1 AND user_id = $2
      LIMIT 1;
      `,
      [fantasyTeamId, userId]
    );

    if (ownerCheck.rowCount === 0) {
      return res.status(403).json({ message: "Forbidden" });
    }

    // ---------- Fetch instance ----------
    const instanceResult = await client.query(
      `
      SELECT *
      FROM fantasydata.fantasy_team_instance
      WHERE fantasy_team_id = $1
        AND match_num = $2
      LIMIT 1;
      `,
      [fantasyTeamId, parsedMatchNum]
    );

    return res.status(200).json(instanceResult.rows[0] ?? null);
  } catch (err) {
    console.error("GET /fantasy-team-instance failed:", err);
    return res.status(500).json({
      message: "Unexpected error occurred"
    });
  } finally {
    client?.release();
  }
});

app.get("/fantasy-team-instance/performances/:fantasyTeamInstanceId", async (req, res) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { fantasyTeamInstanceId } = req.params;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!fantasyTeamInstanceId) {
    return res.status(400).json({ message: "Missing fantasy team instance id" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
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
    WHERE fti.id = $1
),

team_info AS (
    SELECT 
        fti.id AS fti_id,
        ft.id AS fantasy_team_id,
        l.id AS league_id,
        l.season_id,
        fti.match_num,
        fti.player_ids
    FROM fti
    JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
    JOIN fantasydata.leagues l ON l.id = ft.league_id
),

player_seasons AS (
    SELECT DISTINCT
        psi.id AS player_season_id,
        psi.player_id,
        psi.team_id,
        psi.season_id,
        tinfo.match_num
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
)

SELECT 
    ps.player_season_id,
    ps.player_id,

    p.player_name,
    p.full_name,
    p.image AS player_image,

    (
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
    ) AS fantasy_points

FROM player_seasons ps
JOIN irldata.player p ON p.id = ps.player_id

JOIN match_lookup ml
  ON ml.season_id = ps.season_id
 AND (
      (ps.team_id = ml.home_team_id AND ml.home_match_num = ps.match_num)
   OR (ps.team_id = ml.away_team_id AND ml.away_match_num = ps.match_num)
 )

LEFT JOIN irldata.player_performance ppa
  ON ppa.player_season_id = ps.player_season_id
 AND ppa.match_id = ml.match_id;
      `;

    const result = await client.query(sql, [fantasyTeamInstanceId]);

    return res.json(result.rows);

  } catch (err) {
    console.error("FTI performance lookup error:", err);
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