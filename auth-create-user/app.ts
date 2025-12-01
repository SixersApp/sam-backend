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
    const userData = JSON.parse(event.body ?? "");
    const username = event.requestContext.authorizer?.claims?.["cognito:username"];

    if (!userData) {
        return {
            statusCode: 400, body: JSON.stringify({
                error: "Missing user information"
            })
        };
    }

    if (!username) {
        return {
            statusCode: 400, body: JSON.stringify({
                error: "Missing user name in auth token"
            })
        };
    }

    let client;

    try {
        const pool = getPool();
        client = await pool.connect();
        const request = await client.query(`
            SELECT * from authdata.app_user a
            WHERE a.id = $1;
        `, [username]);

        if ((request?.rowCount ?? 0) != 0) {
            return {
                statusCode: 400,
                body: JSON.stringify({
                    message: "user record already exists"
                })
            }
        }
        const profileData = await client.query(`
            SELECT * from authdata.profiles a
            WHERE a.user_id = $1;   
        `, [username]);
        
        if((profileData?.rowCount ?? 0) != 0) {
            return {
                statusCode: 400,
                body: JSON.stringify({
                    message: "user profile already exists"
                })
            }
        }

        await client.query('BEGIN');
        const userQuery = `
            INSERT INTO authdata.app_user (id, email, created_at)
            VALUES ($1, $2, NOW())
            RETURNING id;
        `;

        const userRes = await client.query(userQuery, [username, userData?.email]);

        const profileQuery = `
            INSERT INTO authdata.profiles
            (user_id, full_name, avatar_url, created_at, dob, country, experience, onboarding_stage)
            VALUES ($1, $2, $3, NOW(), $4, $5, $6, $7)
        `;

        await client.query(profileQuery, [
            username,
            userData.fullName ?? null,
            userData.avatar_url ?? null,
            userData.dob ?? null,
            userData.country ?? null,
            userData.experience ?? null,
            userData.onboarding_stage ?? 0
        ]);

        await client.query('COMMIT');
        return {
            statusCode: 200,
            body: JSON.stringify({
                success: true,
                userId: username
            })
        };

    } catch (err) {
        console.log(err);
        await client?.query('ROLLBACK');
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
