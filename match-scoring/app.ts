import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from 'aws-lambda';
import pg, { PoolClient } from 'pg';
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


app.post("/scoring/createMatch", async (req, res) => {
  const tournamentId = req.lambdaEvent.requestContext.authorizer?.claims?.["custom:tournamentId"];
  const match_data = JSON.parse(req.lambdaEvent.body ?? "{}");

  if (tournamentId == null) {
    return res.status(403).json({
      message: "no tournament id provided"
    });
  }

  const required_fields = ["tournament_id", "season_id", "home_team_id", "away_team_id", "match_date"];
  required_fields.forEach(field => {
    if (match_data[field] == undefined) {
      return res.status(403).json({
        message: `Field ${field} is required and missing from message body`
      })
    }
  });

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();
    const tournamentInfo = (await client.query(`
      SELECT * FROM irldata.tournament_info t
      WHERE t.id = $1; 
    `, [match_data.tournament_id])).rows[0];
    const seasonInfo = (await client.query(`
      WITH season_teams 
      AS (
        SELECT DISTINCT t.*
        FROM irldata.player_season_info psi
        JOIN irldata.team t ON psi.team_id = t.id
        WHERE psi.season_id = $1
      )
      SELECT 
            s.*, 
          (SELECT COALESCE(json_agg(season_teams.*), '[]') FROM season_teams) as teams
      FROM irldata.season s
      WHERE s.id = $1;

    `, [match_data.season_id])).rows[0];
    const awayTeamInfo = (await client.query(`
      SELECT t.* FROM irldata.team t WHERE t.id = $1;
    `, [match_data.away_team_id])).rows[0];
    const homeTeamInfo = (await client.query(`
      SELECT t.* FROM irldata.team t WHERE t.id = $1;  
    `, [match_data.home_team_id])).rows[0];

    if(awayTeamInfo == undefined || homeTeamInfo == undefined) {
      return res.status(403).json({ message: "home team and/or away team doesn't exist"});
    }

    if(seasonInfo == undefined) {
      return res.status(403).json({ message: "season doesn't exist" });
    }

    if(tournamentInfo == undefined) {
      return res.status(403).json({ message: "tournament doesn't exist" });
    }

    if(seasonInfo.tournament_id != tournamentInfo.id) {
      return res.status(403).json({ message: "given season doesn't belong in given tournament"});
    }

    if(seasonInfo.teams.find((t: { id: string }) => t.id === homeTeamInfo.id) == undefined) {
      return res.status(403).json({ message: "home team doesn't exist in season"});
    }

    if(seasonInfo.teams.find((t: { id: string }) => t.id === awayTeamInfo.id) == undefined) {
      return res.status(403).json({ message: "away team doesn't exist in season"});
    }

    const response = await client.query(`
      INSERT INTO irldata.match_info
      (
        match_date,
        tournament_id,
        season_id,
        venue_id,
        home_team_id,
        away_team_id,
        inserted_at,
        status,
        home_match_num,
        away_match_num
      )
      VALUES
      (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        NOW(),
        0,
        0
      )
      RETURNING id;
    `, [match_data.match_date, match_data.tournament_id, match_data.season_id, match_data.venue_id, match_data.home_team_id, match_data.away_team_id]);

    return res.json({ id: response.rows[0].id });
  } catch (e) {
    res.status(500).json({
      message: "Unexpected Error Occured"
    });
  } finally {
    client?.release();
  }


  res.json({
    userId: req.lambdaEvent.requestContext.authorizer?.claims?.["sub"],
    message: "Create Match!!"
  })
});

app.patch("/scoring/:matchId/startScoring", (req, res) => {
  res.json({
    userId: req.lambdaEvent.requestContext.authorizer?.claims?.["sub"],
    matchId: req.lambdaEvent.pathParameters?.matchId,
    message: "Start Scoring Match!!"
  })
});

app.post("/scoring/:matchId/addEvent", (req, res) => {
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