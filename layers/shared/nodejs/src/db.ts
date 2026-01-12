import pg from "pg";
import fs from "fs";

let pool: pg.Pool | null = null;

export const getPool = (): pg.Pool => {
  if (pool) {
    return pool;
  }

  const rdsCa = fs.readFileSync("/opt/nodejs/us-west-2-bundle.pem").toString();

  pool = new pg.Pool({
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

export { pg };
