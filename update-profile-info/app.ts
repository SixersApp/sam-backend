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
    const tokenUserId = event.requestContext.authorizer?.claims?.["sub"];
    const userId = event.pathParameters?.userid;

    // --------------------------
    // USER VALIDATION

    if (!userId) {
        return {
            statusCode: 400,
            body: JSON.stringify({ message: "Missing userId in path" })
        };
    }

    // if (tokenUserId !== userId) {
    //     return {
    //         statusCode: 403,
    //         body: JSON.stringify({ message: "Forbidden: user mismatch" })
    //     };
    // }

    // --------------------------
    // PARSE BODY

    const body = JSON.parse(event.body ?? "{}");

    let {
        full_name,
        country,
        dob,
        onboarding_stage,
        experience
    } = body;

    // --------------------------
    // VALIDATE PROVIDED FIELDS
    if (
        full_name === undefined &&
        country === undefined &&
        dob === undefined &&
        onboarding_stage === undefined &&
        experience === undefined
    ) {
        return error(400, "No valid onboarding fields provided");
    }

    // full_name: text
    if (full_name !== undefined && typeof full_name !== "string") {
        return error(400, "full_name must be a string");
    }

    // country: text
    if (country !== undefined && typeof country !== "string") {
        return error(400, "country must be a string");
    }

    // dob: convert to YYYY-MM-DD
    if (dob !== undefined) {
        if (typeof dob !== "string" || isNaN(Date.parse(dob))) {
            return error(400, "dob must be a valid date string (YYYY-MM-DD)");
        }
        dob = new Date(dob).toISOString().split("T")[0];
    }

    // onboarding_stage: integer
    if (onboarding_stage !== undefined) {
        if (typeof onboarding_stage !== "number") {
            return error(400, "onboarding_stage must be an integer");
        }
        onboarding_stage = Math.floor(onboarding_stage);
    }

    // experience: integer
    if (experience !== undefined) {
        if (typeof experience !== "number") {
            return error(400, "experience must be an integer");
        }
        experience = Math.floor(experience);
    }

    // --------------------------
    // BUILD DYNAMIC UPSERT QUERY

    const updates: string[] = [];
    const columns = ["user_id"];
    const placeholders = ["$1"];
    const values: any[] = [userId];

    let index = 2;

    function addField(column: string, value: any) {
        if (value === undefined) return;
        columns.push(column);
        placeholders.push(`$${index}`);
        updates.push(`${column} = EXCLUDED.${column}`);
        values.push(value);
        index++;
    }

    addField("full_name", full_name);
    addField("country", country);
    addField("dob", dob);
    addField("onboarding_stage", onboarding_stage);
    addField("experience", experience);

    const sql = `
        INSERT INTO authdata.profiles (${columns.join(", ")})
        VALUES (${placeholders.join(", ")})
        ON CONFLICT (user_id)
        DO UPDATE SET ${updates.join(", ")};
    `;

    let client;
    try {
        const pool = getPool();
        client = await pool.connect();
        await client.query(sql, values);

        return {
            statusCode: 200,
            body: JSON.stringify({
                message: "Profile updated successfully",
                userId,
                updated_fields: columns.slice(1)
            })
        };

    } catch (err: any) {
        console.error(err);
        return error(500, "Internal server error");
    } finally {
        if (client) client.release();
    }
};

// --------------------------
// Helper
// --------------------------
function error(code: number, message: string): APIGatewayProxyResult {
    return {
        statusCode: code,
        body: JSON.stringify({ message })
    };
}