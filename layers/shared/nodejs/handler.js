"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createHandler = void 0;
const serverless_http_1 = __importDefault(require("serverless-http"));
const createHandler = (app) => {
    return (0, serverless_http_1.default)(app, {
        request: (req, event, context) => {
            req.lambdaEvent = event;
            req.lambdaContext = context;
        }
    });
};
exports.createHandler = createHandler;
