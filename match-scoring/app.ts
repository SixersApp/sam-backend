import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from 'aws-lambda';
import pg from 'pg';
import fs from 'fs';
import express from "express";
import serverless from "serverless-http";
import cors from "cors";
/**
 *
 * Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format
 * @param {Object} event - API Gateway Lambda Proxy Input Format
 *
 * Return doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
 * @returns {Object} object - API Gateway Lambda Proxy Output Format
 *
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

    // --- 👇 ADD THIS LINE ---
    // This forces an encrypted connection without needing the CA file.
    ssl: {
      rejectUnauthorized: true,
      ca: rdsCa
    }
  });
}

const app = express();

app.use(cors());
app.use(express.json());


app.post("/scoring/createMatch", (req, res) => {
  res.json({
    userId: req.lambdaEvent.requestContext.authorizer?.claims?.["sub"],
    message: "Create Match!!"
  })
});

app.patch("/scoring/{matchId}/startScoring", (req, res) => {
  res.json({
    userId: req.lambdaEvent.requestContext.authorizer?.claims?.["sub"],
    matchId: req.lambdaEvent.pathParameters?.matchId,
    message: "Start Scoring Match!!"
  })
});

app.post("/scoring/{matchId}/addEvent", (req, res) => {
  res.json({
    userId: req.lambdaEvent.requestContext.authorizer?.claims?.["sub"],
    matchId: req.lambdaEvent.pathParameters?.matchId,
    message: "Add Match Event!!"
  })
});

export const lambdaHandler = serverless(app, {
  request: (req: any, event: APIGatewayProxyEvent, context: Context) => {
    req.lambdaEvent = event;
    req.lambdaContext = context;
  }
});