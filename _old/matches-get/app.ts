import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from "aws-lambda";
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
  const rdsCa = fs.readFileSync('/opt/nodejs/us-west-2-bundle.pem').toString();

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
   GET ALL LIVE / UPCOMING MATCHES FOR USER BASED ON THEIR LEAGUE'S TOURNAMENTS
   ======================================================================================= */
app.get("/matches/homeFeed", async (req, res) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      SELECT 
          m.id,
          m.match_date,
          m.tournament_id,
          t.name AS tournament_name,
          m.season_id,
          m.venue_id,
          m.home_team_id,
          m.away_team_id,
          m.home_team_score,
          m.away_team_score,
          m.home_team_wickets,
          m.away_team_wickets,
          m.home_team_balls,
          m.away_team_balls,
          m.dls,
          m.status,
          ht.name AS home_team_name,
          ht.image AS home_team_image,
          at.name AS away_team_name,
          at.image AS away_team_image
      FROM irldata.match_info m
      JOIN irldata.team ht ON ht.id = m.home_team_id
      JOIN irldata.team at ON at.id = m.away_team_id
      JOIN irldata.tournament_info t ON t.id = m.tournament_id
      WHERE m.tournament_id IN (
          SELECT DISTINCT l.tournament_id
          FROM fantasydata.fantasy_teams ft
          JOIN fantasydata.leagues l
            ON l.id = ft.league_id
          WHERE ft.user_id = $1
      )
      AND m.match_date >= NOW()::date
      ORDER BY 
          CASE WHEN m.status = 'Live' THEN 0 ELSE 1 END,
          m.match_date ASC
      LIMIT 20;
    `;

    const result = await client.query(sql, [userId]);

    return res.json(result.rows);
  } catch (err) {
    console.error("match feed error", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET SPECIFIC MATCH DETAILS BY MATCH ID
   ======================================================================================= */
app.get("/matches/:matchId", async (req, res) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { matchId } = req.params;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!matchId) {
    return res.status(400).json({ message: "Match ID is required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      SELECT 
          m.id,
          m.match_date,
          m.tournament_id,
          m.season_id,
          m.venue_id,
          m.home_team_id,
          m.away_team_id,
          m.home_team_score,
          m.away_team_score,
          m.home_team_wickets,
          m.away_team_wickets,
          m.home_team_balls,
          m.away_team_balls,
          m.dls,
          m.inserted_at,
          m.status,
          m.home_match_num,
          m.away_match_num,
          ht.name AS home_team_name,
          ht.image AS home_team_image,
          at.name AS away_team_name,
          at.image AS away_team_image
      FROM irldata.match_info m
      JOIN irldata.team ht ON ht.id = m.home_team_id
      JOIN irldata.team at ON at.id = m.away_team_id
      WHERE m.id = $1;
    `;

    const result = await client.query(sql, [matchId]);

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Match not found" });
    }

    return res.json(result.rows[0]);
  } catch (err) {
    console.error("match details error", err);
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