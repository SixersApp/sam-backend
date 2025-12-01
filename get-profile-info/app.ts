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
    const tokenUserId = event.requestContext.authorizer?.claims?.["sub"];
    const userId = event.pathParameters?.userid;

    if (!userId) {
        return {
            statusCode: 401,
            body: JSON.stringify({ message: "Missing userId in path" })
        };
    }

    if (tokenUserId !== userId) {
        return {
            statusCode: 403,
            body: JSON.stringify({ message: "Forbidden: user mismatch" })
        };
    }

    let client;

    try {
        const pool = getPool();
        client = await pool.connect();

        const result = await client.query(
            `SELECT 
                user_id,
                full_name,
                country,
                dob,
                onboarding_stage,
                experience,
                created_at
             FROM authdata.profiles
             WHERE user_id = $1`,
            [userId]
        );

        if (result.rowCount === 0) {
            return {
                statusCode: 404,
                body: JSON.stringify({ message: "Profile not found" })
            };
        }

        return {
            statusCode: 200,
            body: JSON.stringify(result.rows[0])
        };

    } catch (err: any) {
        console.error(err);
        return {
            statusCode: 500,
            body: JSON.stringify({ 
                message: "Internal server error",
                error: err.message
            })
        };
    } finally {
        if (client) client.release();
    }
};