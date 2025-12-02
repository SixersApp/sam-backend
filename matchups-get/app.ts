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
  const userId = event.requestContext.authorizer?.claims?.["sub"];

  // Must be logged in
  if (!userId) {
    return {
      statusCode: 401,
      body: JSON.stringify({ message: "Unauthorized" }),
    };
  }

  // Require ?match_num=#
  const matchNumStr = event.queryStringParameters?.match_num;
  if (!matchNumStr) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        message: "match_num query parameter is required",
      }),
    };
  }

  const matchNum = Number(matchNumStr);
  if (isNaN(matchNum)) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        message: "match_num must be a valid number",
      }),
    };
  }

  const client = await getPool().connect();

  try {
    const result = await client.query(
      `
      SELECT
        fm.id,
        fm.league_id,
        fm.match_num,

        -- Team Instance 1
        fti1.id AS fantasy_team_instance1_id,
        ft1.id  AS fantasy_team1_id,
        ft1.team_name AS fantasy_team1_name,
        fm.fantasy_team_instance1_score,

        -- Team Instance 2
        fti2.id AS fantasy_team_instance2_id,
        ft2.id  AS fantasy_team2_id,
        ft2.team_name AS fantasy_team2_name,
        fm.fantasy_team_instance2_score

      FROM fantasydata.fantasy_matchups fm

      -- Instance 1 → Team 1 → User
      JOIN fantasydata.fantasy_team_instance fti1
        ON fti1.id = fm.fantasy_team_instance1_id
      JOIN fantasydata.fantasy_teams ft1
        ON ft1.id = fti1.fantasy_team_id

      -- Instance 2 → Team 2
      JOIN fantasydata.fantasy_team_instance fti2
        ON fti2.id = fm.fantasy_team_instance2_id
      JOIN fantasydata.fantasy_teams ft2
        ON ft2.id = fti2.fantasy_team_id

      WHERE 
        (ft1.user_id = $1 OR ft2.user_id = $1)
        AND fm.match_num = $2

      ORDER BY fm.match_num DESC;
      `,
      [userId, matchNum]
    );

    return {
      statusCode: 200,
      body: JSON.stringify(result.rows),
    };
  } catch (err) {
    console.error("Matchups error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ message: "Internal server error" }),
    };
  } finally {
    client.release();
  }
};