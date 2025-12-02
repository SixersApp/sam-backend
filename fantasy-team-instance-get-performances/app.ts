import { APIGatewayProxyEvent, APIGatewayProxyResult, Context } from "aws-lambda";
import pg from "pg";
import fs from "fs";
import express from "express";
import serverless from "serverless-http";
import cors from "cors";

/**
 * Extend Express Request to include Lambda event/context
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
    ssl: {
      rejectUnauthorized: true,
      ca: rdsCa
    }
  });
};

const app = express();
app.use(cors());
app.use(express.json());


app.get("/fantasyTeamInstance/:ftiId/performances", async (req, res) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { ftiId } = req.params;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!ftiId) {
    return res.status(400).json({ message: "Missing fantasy team instance id" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      WITH fti AS (
          SELECT 
              fti.id,
              fti.fantasy_team_id,
              fti.match_num,
              ARRAY[
                  bat1, bat2, bat3, bat4,
                  bowl1, bowl2, bowl3, bowl4,
                  all1, all2, all3,
                  wicket1, wicket2,
                  bench1, bench2, bench3, bench4, bench5, bench6, bench7, bench8,
                  flex1, flex2, flex3, flex4
              ] AS player_ids
          FROM fantasydata.fantasy_team_instance fti
          WHERE fti.id = $1
      ),

      team_info AS (
          SELECT 
              fti.id AS fti_id,
              ft.id AS fantasy_team_id,
              l.id AS league_id,
              l.season_id,
              fti.match_num,
              fti.player_ids
          FROM fti
          JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
          JOIN fantasydata.leagues l ON l.id = ft.league_id
      ),

      player_seasons AS (
          SELECT 
              psi.id AS player_season_id,
              psi.player_id,
              psi.team_id,
              psi.season_id,
              tinfo.match_num,
              tinfo.league_id
          FROM irldata.player_season_info psi
          JOIN team_info tinfo ON psi.season_id = tinfo.season_id
          WHERE psi.player_id = ANY(tinfo.player_ids)
      ),

      match_lookup AS (
          SELECT 
              mi.id AS match_id,
              mi.season_id,
              mi.home_team_id,
              mi.away_team_id,
              mi.home_match_num,
              mi.away_match_num
          FROM irldata.match_info mi
      )

      SELECT 
          ps.player_season_id,
          ps.player_id,

          ppa.id AS performance_id,
          ppa.runs_scored,
          ppa.balls_faced,
          ppa.fours,
          ppa.sixes,
          ppa.runs_conceded,
          ppa.balls_bowled,
          ppa.wickets_taken,
          ppa.catches,
          ppa.dismissals,
          ppa.caught_behinds,
          ppa.wides_bowled,
          ppa.byes_bowled,
          ppa.run_outs,
          ppa.no_balls_bowled,
          ppa.catches_dropped,
          ppa.inserted_at,

          ml.home_team_id,
          ht.name AS home_team_name,
          ht.image AS home_team_image,

          ml.away_team_id,
          at.name AS away_team_name,
          at.image AS away_team_image

      FROM player_seasons ps

      JOIN match_lookup ml
          ON ml.season_id = ps.season_id
          AND (
              (ps.team_id = ml.home_team_id AND ml.home_match_num = ps.match_num)
              OR
              (ps.team_id = ml.away_team_id AND ml.away_match_num = ps.match_num)
          )

      LEFT JOIN irldata.player_performance ppa
          ON ppa.player_season_id = ps.player_season_id
          AND ppa.match_id = ml.match_id

      LEFT JOIN irldata.team ht ON ht.id = ml.home_team_id
      LEFT JOIN irldata.team at ON at.id = ml.away_team_id;
    `;

    const result = await client.query(sql, [ftiId]);

    return res.json(result.rows);

  } catch (err) {
    console.error("FTI performance lookup error:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* ======================================================================
   EXPORT LAMBDA HANDLER
   ====================================================================== */

export const lambdaHandler = serverless(app, {
  request: (req: any, event: APIGatewayProxyEvent, context: Context) => {
    req.lambdaEvent = event;
    req.lambdaContext = context;
  },
});