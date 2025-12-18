import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import pg from 'pg';
import fs from 'fs';

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

export const lambdaHandler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  if (!event.body) {
    return {
      statusCode: 400,
      body: JSON.stringify({ error: 'Missing request body' })
    };
  }

  const userData = JSON.parse(event.body);
  const username =
    event.requestContext.authorizer?.claims?.['cognito:username'];

  if (!username) {
    return {
      statusCode: 401,
      body: JSON.stringify({ error: 'Unauthorized' })
    };
  }

  let client: pg.PoolClient | undefined;

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

    return {
      statusCode: 200,
      body: JSON.stringify({
        success: true,
        userId: username
      })
    };
  } catch (err) {
    console.error('PUT /profile failed:', err);
    await client?.query('ROLLBACK');

    return {
      statusCode: 500,
      body: JSON.stringify({
        error: 'Internal server error'
      })
    };
  } finally {
    client?.release();
  }
};

