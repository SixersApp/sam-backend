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

/* =======================================================================================
   EXPORT LAMBDA HANDLER
   ======================================================================================= */
export const lambdaHandler = serverless(app, {
  request: (req: any, event: APIGatewayProxyEvent, context: Context) => {
    req.lambdaEvent = event;
    req.lambdaContext = context;
  }
});