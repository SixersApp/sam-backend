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



export const lambdaHandler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  const userId = event.requestContext.authorizer?.claims?.["sub"];

  // Must be logged in
  if (!userId) {
    return {
      statusCode: 401,
      body: JSON.stringify({ message: "Unauthorized" }),
    };
  }

  // Require ?match_num=#
  const matchNumStr = event.queryStringParameters?.match_num;
  if (!matchNumStr) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        message: "match_num query parameter is required",
      }),
    };
  }

  const matchNum = Number(matchNumStr);
  if (isNaN(matchNum)) {
    return {
      statusCode: 400,
      body: JSON.stringify({
        message: "match_num must be a valid number",
      }),
    };
  }

  const client = await getPool().connect();

  try {
    const result = await client.query(
      `
            WITH scoring AS (

          -- ============================================
          --  (1) COPY YOUR FULL SCORING CALCULATION LOGIC
          --      but parameterize by fti_id
          -- ============================================
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
                  tinfo.league_id,
                  tinfo.fti_id
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
              ps.fti_id,
              SUM(
                  -- 1. FIELDING
                  (COALESCE(ppa.catches, 0) * 8) +
                  (CASE WHEN COALESCE(ppa.catches, 0) >= 3 THEN 4 ELSE 0 END) +

                  -- 2. BATTING
                  (COALESCE(ppa.runs_scored, 0) * 1) + 
                  (COALESCE(ppa.fours, 0) * 1) +
                  (COALESCE(ppa.sixes, 0) * 2) +
                  (CASE WHEN COALESCE(ppa.runs_scored, 0) > 50 THEN 8 ELSE 0 END) +
                  (CASE WHEN COALESCE(ppa.runs_scored, 0) > 100 THEN 8 ELSE 0 END) +

                  -- Batting Strike Rate
                  (CASE 
                    WHEN COALESCE(ppa.balls_faced, 0) = 0 THEN 0 
                    ELSE CASE 
                      WHEN (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) BETWEEN 0 AND 30 THEN -6
                      WHEN (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) > 30 AND (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) <= 39 THEN -4
                      WHEN (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) >= 40 AND (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) <= 50 THEN -2
                      WHEN (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) >= 100 AND (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) <= 119 THEN 2
                      WHEN (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) >= 120 AND (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) <= 139 THEN 4
                      WHEN (COALESCE(ppa.runs_scored, 0) * 100.0 / ppa.balls_faced) >= 140 THEN 6
                      ELSE 0
                    END
                  END) +

                  -- 3. BOWLING
                  (COALESCE(ppa.wickets_taken, 0) * 25) +
                  (CASE WHEN COALESCE(ppa.wickets_taken, 0) > 3 THEN 4 ELSE 0 END) +
                  (CASE WHEN COALESCE(ppa.wickets_taken, 0) > 5 THEN 5 ELSE 0 END) +

                  -- Bowling Economy
                  (CASE 
                    WHEN COALESCE(ppa.balls_bowled, 0) = 0 THEN 0 
                    ELSE CASE 
                      WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) BETWEEN 0 AND 2.5 THEN 6
                      WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) > 2.5 AND (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) <= 3.49 THEN 4
                      WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) >= 3.5 AND (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) <= 4.5 THEN 2
                      WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) >= 7 AND (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) <= 8 THEN -2
                      WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) > 8 AND (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) <= 9 THEN -4
                      WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) > 9 THEN -6
                      ELSE 0
                    END
                  END)
              ) AS total_points
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
          GROUP BY ps.fti_id
      )

      -- =======================================================
      --     FINAL MATCHUP QUERY WITH CALCULATED SCORES
      -- =======================================================
      SELECT
          fm.id,
          fm.league_id,
          fm.match_num,

          -- Instance 1
          fti1.id AS fantasy_team_instance1_id,
          ft1.id  AS fantasy_team1_id,
          ft1.team_name AS fantasy_team1_name,
          COALESCE(s1.total_points, 0) AS fantasy_team_instance1_score,

          -- Instance 2
          fti2.id AS fantasy_team_instance2_id,
          ft2.id  AS fantasy_team2_id,
          ft2.team_name AS fantasy_team2_name,
          COALESCE(s2.total_points, 0) AS fantasy_team_instance2_score

      FROM fantasydata.fantasy_matchups fm

      JOIN fantasydata.fantasy_team_instance fti1
        ON fti1.id = fm.fantasy_team_instance1_id
      JOIN fantasydata.fantasy_teams ft1
        ON ft1.id = fti1.fantasy_team_id

      JOIN fantasydata.fantasy_team_instance fti2
        ON fti2.id = fm.fantasy_team_instance2_id
      JOIN fantasydata.fantasy_teams ft2
        ON ft2.id = fti2.fantasy_team_id

      LEFT JOIN scoring s1 ON s1.fti_id = fti1.id
      LEFT JOIN scoring s2 ON s2.fti_id = fti2.id

      WHERE 
          (ft1.user_id = $1 OR ft2.user_id = $1)
          AND fm.match_num = $2

      ORDER BY fm.match_num DESC;
      `,
      [userId, matchNum]
    );

    return {
      statusCode: 200,
      body: JSON.stringify(result.rows),
    };
  } catch (err) {
    console.error("Matchups error:", err);
    return {
      statusCode: 500,
      body: JSON.stringify({ message: "Internal server error" }),
    };
  } finally {
    client.release();
  }
};