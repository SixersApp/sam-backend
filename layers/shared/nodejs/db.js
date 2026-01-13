"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.pg = exports.getPool = void 0;
const pg_1 = __importDefault(require("pg"));
exports.pg = pg_1.default;
const fs_1 = __importDefault(require("fs"));
let pool = null;
const getPool = () => {
    if (pool) {
        return pool;
    }
    const rdsCa = fs_1.default.readFileSync("/opt/nodejs/us-west-2-bundle.pem").toString();
    pool = new pg_1.default.Pool({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_NAME,
        max: 1,
        idleTimeoutMillis: 5000,
        connectionTimeoutMillis: 5000,
        ssl: {
            rejectUnauthorized: true,
            ca: rdsCa
        }
    });
    return pool;
};
exports.getPool = getPool;
