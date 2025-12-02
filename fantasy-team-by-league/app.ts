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

  const leagueId = event.queryStringParameters?.leagueId;
  const userId = event.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return { statusCode: 401, body: JSON.stringify({ message: "Unauthorized" }) };
  }

  if (!leagueId) {
    return { statusCode: 400, body: JSON.stringify({ message: "Missing leagueId" }) };
  }

  const client = await getPool().connect();
  try {
    const result = await client.query(
      `
      SELECT *
      FROM fantasydata.fantasy_teams
      WHERE user_id = $1 AND league_id = $2
      LIMIT 1;
      `,
      [userId, leagueId]
    );

    return {
      statusCode: 200,
      body: JSON.stringify(result.rows[0] ?? null)
    };
  } finally {
    client.release();
  }
};