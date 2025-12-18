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

/* =======================================================================================
   DATABASE
   ======================================================================================= */

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

/* =======================================================================================
   EXPRESS APP
   ======================================================================================= */

const app = express();
app.use(cors());
app.use(express.json());

/* =======================================================================================
   GET ALL LEAGUES FOR AUTHENTICATED USER
   GET /leagues
   ======================================================================================= */

app.get("/leagues", async (req, res) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client: pg.PoolClient | null = null;

  try {
    client = await getPool().connect();

    const result = await client.query(
      `
      SELECT DISTINCT
        l.id,
        l.name,
        l.tournament_id,
        l.creator_id,
        l.status,
        l.max_teams,
        l.join_code,
        l.season_id
      FROM fantasydata.leagues l
      JOIN fantasydata.fantasy_teams ft
        ON ft.league_id = l.id
      WHERE ft.user_id = $1
      ORDER BY l.name ASC;
      `,
      [userId]
    );

    return res.status(200).json(result.rows);
  } catch (err) {
    console.error("GET /leagues/user failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET LEAGUE SCORING RULES FOR A FANTASY TEAM INSTANCE
   GET /leagues/scoring-rules/:fantasyTeamInstanceId
   ======================================================================================= */

app.get("/leagues/scoring-rules/:fantasyTeamId", async (req, res) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { fantasyTeamId } = req.params;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!fantasyTeamId) {
    return res.status(400).json({ message: "Missing fantasy team ID" });
  }

  let client: pg.PoolClient | null = null;

  try {
    client = await getPool().connect();

    const sql = `
      WITH team_and_league AS (
        SELECT 
          ft.id AS fantasy_team_id,
          l.id AS league_id
        FROM fantasydata.fantasy_teams ft
        JOIN fantasydata.leagues l
          ON l.id = ft.league_id
        WHERE ft.id = $1
          AND ft.user_id = $2
      )
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
      FROM team_and_league tl
      JOIN fantasydata.league_scoring_rules lsr
        ON lsr.league_id = tl.league_id
      ORDER BY lsr.category, lsr.stat;
    `;

    const result = await client.query(sql, [fantasyTeamId, userId]);

    if (result.rowCount === 0) {
      return res.status(404).json({
        message: "Fantasy team not found or access denied"
      });
    }

    return res.status(200).json(result.rows);
  } catch (err) {
    console.error(
      "GET /leagues/scoring-rules/:fantasyTeamId failed:",
      err
    );
    return res.status(500).json({
      message: "Unexpected error occurred",
    });
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