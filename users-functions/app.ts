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
   CREATE OR UPDATE USER AUTH AND DEFAULT PROFILE DATA
   ======================================================================================= */
app.put("/auth/signup", async (req, res) => {
  if (!req.body) {
    return res.status(401).json({ message: "Missing Request Body" });
  }

  const userData =
    typeof req.body === 'string'
      ? JSON.parse(req.body)
      : req.body;
  const username = req.lambdaEvent.requestContext.authorizer?.claims?.["cognito:username"];

  if (!username) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    await client.query('BEGIN');

    // ---------- UPSERT app_user ----------
    await client.query(
      `
      INSERT INTO authdata.app_user (id, email, created_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT (id)
      DO UPDATE SET
        email = EXCLUDED.email;
      `,
      [username, userData.email]
    );

    // ---------- UPSERT profile ----------
    await client.query(
      `
      INSERT INTO authdata.profiles
      (user_id, full_name, avatar_url, dob, country, experience, onboarding_stage, created_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
      ON CONFLICT (user_id)
      DO UPDATE SET
        full_name = EXCLUDED.full_name,
        avatar_url = EXCLUDED.avatar_url,
        dob = EXCLUDED.dob,
        country = EXCLUDED.country,
        experience = EXCLUDED.experience,
        onboarding_stage = EXCLUDED.onboarding_stage;
      `,
      [
        username,
        userData.fullName ?? null,
        userData.avatar_url ?? null,
        userData.dob ?? null,
        userData.country ?? null,
        userData.experience ?? null,
        userData.onboarding_stage ?? 0
      ]
    );

    await client.query('COMMIT');

    return res.status(200).json({ message: "User profile created/updated successfully", userId: username });
  } catch (err) {
    console.error('PUT /profile failed:', err);
    await client?.query('ROLLBACK');

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
  }
});