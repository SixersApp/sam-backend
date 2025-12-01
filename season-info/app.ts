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


export const lambdaHandler = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
    const tournamentId = event.requestContext?.authorizer?.claims["custom:tournamentId"];
    const seasonId = event.pathParameters?.seasonId;

    if (tournamentId == null) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                message: "tournamentId for this user is null"
            })
        }
    }

    if(seasonId == undefined) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                message: "null or empty seasonId"
            })
        }
    };

    let client;

    try {
        const pool = getPool();
        client = await pool.connect();
        const result = await client.query(`
            WITH season_teams AS (
                SELECT DISTINCT t.*
                FROM irldata.player_season_info psi
                JOIN irldata.team t ON psi.team_id = t.id
                WHERE psi.season_id = $1
            ),
            season_matches AS (
                SELECT m.*
                FROM irldata.match_info m
                WHERE m.season_id = $1
            )
            SELECT 
	            s.*,
                (SELECT COALESCE(json_agg(season_teams.*), '[]') FROM season_teams) as teams,
                (SELECT COALESCE(json_agg(season_matches.*), '[]') FROM season_matches) as matches
            FROM irldata.season s
            WHERE s.id = $1;
            `, [seasonId]);

        if ((result.rowCount ?? 0) > 0) {
            if(result.rows[0].tournament_id !== tournamentId) {
                return {
                    statusCode: 400,
                    body: JSON.stringify({
                        message: "Attempting to acccess season info for unauthorized tournament"
                    })
                }
            };
            return {
                statusCode: 200,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(result.rows[0])
            };
        }
        return {
            statusCode: 204,
            body: JSON.stringify({
                message: "No rows were found that matched this season"
            })
        }
    } catch (err) {
        console.log(err);
        return {
            statusCode: 500,
            body: JSON.stringify({
                message: 'some error happened',
            }),
        };
    } finally {
        if (client) {
            client.release();
        }
    }
};
