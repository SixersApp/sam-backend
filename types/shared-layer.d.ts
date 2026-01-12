declare module "/opt/nodejs/index" {
  import { Express, Request, Response, NextFunction } from "express";
  import { Pool } from "pg";
  import { APIGatewayProxyEvent, Context, Handler } from "aws-lambda";

  export function getPool(): Pool;
  export function createApp(): Express;
  export function createHandler(app: Express): Handler;

  export { Express, Request, Response, NextFunction } from "express";
  export { Pool } from "pg";
  export { APIGatewayProxyEvent, Context } from "aws-lambda";
}

declare global {
  namespace Express {
    interface Request {
      lambdaEvent: import("aws-lambda").APIGatewayProxyEvent;
      lambdaContext: import("aws-lambda").Context;
    }
  }
}

export {};
