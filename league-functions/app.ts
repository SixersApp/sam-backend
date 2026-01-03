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
app.get("/leagues", async (req, res) => {

  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return {
      statusCode: 401,
      body: JSON.stringify({ message: "Unauthorized" })
    };
  }

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();
    const result = await client.query(
      `
      SELECT 
    l.id,
    l.name,
    l.tournament_id,
    l.creator_id,
    l.status,
    l.max_teams,
    l.join_code,
    l.season_id,

	ft.id as user_team_id,
    ti.abbreviation AS tournament_abbr,

    s.end_year AS season_year,

    (
        SELECT MAX(fm.match_num)
        FROM fantasydata.fantasy_team_instance fti
        JOIN fantasydata.fantasy_matchups fm 
            ON fm.fantasy_team_instance1_id = fti.id 
            OR fm.fantasy_team_instance2_id = fti.id
        WHERE fti.fantasy_team_id = ft.id
    ) AS latest_game,
    (
        SELECT json_agg(
            to_jsonb(all_ft) || jsonb_build_object('user_name', p.full_name)
        )
        FROM fantasydata.fantasy_teams all_ft
        JOIN authdata.profiles p ON p.user_id = all_ft.user_id
        WHERE all_ft.league_id = l.id
    ) AS teams

FROM fantasydata.leagues l
JOIN fantasydata.fantasy_teams ft 
    ON ft.league_id = l.id 
    AND ft.user_id = $1
JOIN irldata.tournament_info ti 
    ON ti.id = l.tournament_id
JOIN irldata.season s 
    ON s.id = l.season_id

ORDER BY l.name ASC;
      `,
      [userId]
    );
    return res.status(200).json(result.rows);
  } catch (err) {
    console.error("GET /leagues failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET SPECIFIC MATCH DETAILS
   GET /matches/:matchId
   ======================================================================================= */
app.get("/leagues/:leagueId", async (req, res) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { leagueId } = req.params;
  if (!leagueId) {
    return res.status(400).json({ message: "League Id is required" });
  }

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }


  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
        SELECT 
            l.id,
            l.name,
            l.tournament_id,
            l.creator_id,
            l.status,
            l.max_teams,
            l.join_code,
            l.season_id
        FROM fantasydata.leagues l
        JOIN fantasydata.fantasy_teams ft ON ft.league_id = l.id
        WHERE ft.user_id = $1
          AND l.id = $2;
    `;

    const result = await client.query(sql, [userId, leagueId]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "Match not found or you do not have access" });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /matches/:matchId failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

app.get("/leagues/scoring-rules", async (req, res) => {
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
        SELECT 
            lsr.id,
            lsr.league_id,
            lsr.stat,
            lsr.category,
            lsr.mode,
            lsr.per_unit_points,
            lsr.flat_points,
            lsr.threshold,
            lsr.band,
            lsr.multiplier,
            lsr.created_at
        FROM fantasydata.league_scoring_rules lsr
        WHERE lsr.league_id IS NULL
        ORDER BY lsr.category, lsr.stat;
    `;

    const result = await client.query(sql);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "No Default Scoring Rules Found" });
    }

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /leagues/scoring-rules failed", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});


app.get("/leagues/:leagueId/scoring-rules", async (req, res) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  const { leagueId } = req.params;
  if (!leagueId) {
    return res.status(400).json({ message: "League Id is required" });
  }


  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
        SELECT 
            lsr.id,
            lsr.league_id,
            lsr.stat,
            lsr.category,
            lsr.mode,
            lsr.per_unit_points,
            lsr.flat_points,
            lsr.threshold,
            lsr.band,
            lsr.multiplier,
            lsr.created_at
        FROM fantasydata.league_scoring_rules lsr
        WHERE lsr.league_id = $1
        ORDER BY lsr.category, lsr.stat;
    `;

    const result = await client.query(sql, [leagueId]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "No Default Scoring Rules Found" });
    }

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /leagues/{leagueId}/scoring-rules failed", err);
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