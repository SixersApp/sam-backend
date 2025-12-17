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
   GET SEASON INFO (TEAMS + MATCHES)
   GET /seasons/:seasonId
   ======================================================================================= */
app.get("/seasons/:seasonId", async (req, res) => {
  const { seasonId } = req.params;

  // --------------------------
  // AUTH / VALIDATION

  if (!seasonId) {
    return res.status(400).json({
      message: "null or empty seasonId"
    });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT id, tournament_id, start_year, end_year
      FROM irldata.season
      WHERE id = $1
      LIMIT 1;
      `,
      [seasonId]
    );

    if ((result.rowCount ?? 0) === 0) {
      return res.status(204).json({
        message: "No rows were found that matched this season"
      });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /seasons/:seasonId failed:", err);
    return res.status(500).json({
      message: "Internal server error"
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
  }
});