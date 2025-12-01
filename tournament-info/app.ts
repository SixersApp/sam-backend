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

    if (tournamentId == null) {
        return {
            statusCode: 400,
            body: JSON.stringify({
                message: "tournamentId for this user is null"
            })
        }
    }

    let client;

    try {
        const pool = getPool();
        client = await pool.connect();
        const result = await client.query(`
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
            `, [tournamentId]);

        if ((result.rowCount ?? 0) > 0) {
            return {
                statusCode: 200,
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(result.rows[0])
            };
        }
        return {
            statusCode: 204,
            body: JSON.stringify({
                message: "No rows were found that matched this tournament id"
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
