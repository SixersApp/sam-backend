import { Express } from "express";
import { APIGatewayProxyEvent, Context } from "aws-lambda";
import serverless from "serverless-http";

export const createHandler = (app: Express) => {
  return serverless(app, {
    request: (req: any, event: APIGatewayProxyEvent, context: Context) => {
      req.lambdaEvent = event;
      req.lambdaContext = context;
    }
  });
};
