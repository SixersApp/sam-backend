import { APIGatewayProxyEvent, Context } from "aws-lambda";

declare global {
  namespace Express {
    interface Request {
      lambdaEvent: APIGatewayProxyEvent;
      lambdaContext: Context;
    }
  }
}

export { APIGatewayProxyEvent, Context };
