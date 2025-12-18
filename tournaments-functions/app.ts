import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from "aws-lambda";
import pg from "pg";
import fs from "fs";
import express from "express";
import serverless from "serverless-http";
import cors from "cors";
import { start } from "repl";

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
   GET TOURNAMENT INFO (PATH PARAMETER BASED)
   ======================================================================================= */
app.get("/tournaments/:tournamentId", async (req, res) => {

  const { tournamentId } = req.params;

  const tokenUserId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  // --------------------------
  // AUTH / VALIDATION

  if (!tokenUserId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!tournamentId) {
    return res.status(400).json({
      message: "Missing tournamentId in path"
    });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT 
        t.*, 
        (
          SELECT json_agg(s.*)
          FROM irldata.season s
          WHERE s.tournament_id = t.id
        ) AS seasons,
        (
          SELECT json_agg(v.*)
          FROM irldata.venue_info v
          WHERE v.tournament_id = t.id
        ) AS venues
      FROM irldata.tournament_info t
      WHERE t.id = $1;
      `,
      [tournamentId]
    );

    if ((result.rowCount ?? 0) === 0) {
      return res.status(404).json({
        message: "No rows were found that matched this tournament id"
      });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /tournaments/:tournamentId failed:", err);
    return res.status(500).json({
      message: "Internal server error"
    });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET SEASONS FOR TOURNAMENT
   GET /tournaments/:tournamentId/seasons
   ======================================================================================= */
app.get("/tournaments/:tournamentId/seasons", async (req, res) => {

    const { tournamentId} = req.params;

    const tokenUserId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

    // --------------------------
    // AUTH / VALIDATION

    if (!tokenUserId) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    if (!tournamentId) {
      return res.status(400).json({
        message: "Missing path parameters"
      });
    }

    let client;

    try {
      const pool = getPool();
      client = await pool.connect();

      const result = await client.query(
        `
        SELECT id, start_year, end_year
        FROM irldata.season
        WHERE tournament_id = $1
        ORDER BY start_year;
        `,
        [tournamentId]
      );

      return res.status(200).json({
        tournamentId,
        seasons: result.rows.map(row => ({
          id: row.id,
          start_year: row.start_year,
          end_year: row.end_year
        }))
      });

    } catch (err) {
      console.error(
        "GET /tournaments/:tournamentId/seasons failed:",
        err
      );
      return res.status(500).json({
        message: "Internal server error"
      });
    } finally {
      client?.release();
    }
  }
);

/* =======================================================================================
   EXPORT LAMBDA HANDLER
   ======================================================================================= */
export const lambdaHandler = serverless(app, {
  request: (req: any, event: APIGatewayProxyEvent, context: Context) => {
    req.lambdaEvent = event;
    req.lambdaContext = context;
  }
});