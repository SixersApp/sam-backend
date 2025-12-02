import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import pg from 'pg';
import fs from 'fs';
import path from 'path';

/**
 *
 * Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format
 * @param {Object} event - API Gateway Lambda Proxy Input Format
 *
 * Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
 * @returns {Object} object - API Gateway Lambda Proxy Output Format
 *
 */
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

        // --- 👇 ADD THIS LINE ---
        // This forces an encrypted connection without needing the CA file.
        ssl: {
            rejectUnauthorized: true,
            ca: rdsCa
        }
    });
}



export const lambdaHandler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {

  const teamId = event.queryStringParameters?.teamId;
  const matchNumStr = event.queryStringParameters?.matchNum;
  const userId = event.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return { statusCode: 401, body: JSON.stringify({ message: "Unauthorized" }) };
  }

  if (!teamId || !matchNumStr) {
    return { statusCode: 400, body: JSON.stringify({ message: "Missing teamId or matchNum" }) };
  }

  const matchNum = Number(matchNumStr);

  const client = await getPool().connect();
  try {
    // Validate ownership: ensure user owns the fantasy team
    const ownerCheck = await client.query(
      `
      SELECT 1
      FROM fantasydata.fantasy_teams
      WHERE id = $1 AND user_id = $2
      LIMIT 1;
      `,
      [teamId, userId]
    );

    if (ownerCheck.rowCount === 0) {
      return { statusCode: 403, body: JSON.stringify({ message: "Forbidden" }) };
    }

    // Return the instance
    const instance = await client.query(
      `
      SELECT *
      FROM fantasydata.fantasy_team_instance
      WHERE fantasy_team_id = $1
      AND match_num = $2
      LIMIT 1;
      `,
      [teamId, matchNum]
    );

    return {
      statusCode: 200,
      body: JSON.stringify(instance.rows[0] ?? null)
    };
  } finally {
    client.release();
  }
};