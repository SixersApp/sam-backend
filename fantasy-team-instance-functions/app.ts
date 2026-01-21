import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   GET FANTASY TEAM INSTANCE
   GET /fantasy-team-instance?teamId=<teamId>&match_num=<match_num>
   ======================================================================================= */
app.get("/fantasy-team-instance", async (req: Request, res: Response) => {
  const teamId = req.query.teamId as string | undefined;
  const matchNum = req.query.match_num as string | undefined;
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!teamId) {
    return res.status(400).json({ message: "Missing teamId query parameter" });
  }

  if (!matchNum) {
    return res.status(400).json({ message: "Missing match_num query parameter" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT fti.*
      FROM fantasydata.fantasy_team_instance fti
      JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
      WHERE fti.fantasy_team_id = $1 
        AND fti.match_num = $2
        AND ft.user_id = $3
      LIMIT 1;
      `,
      [teamId, matchNum, userId]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({
        message: "Fantasy team instance not found or you do not have access to it"
      });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /fantasy-team-instance failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET FANTASY TEAM INSTANCE PERFORMANCES
   GET /fantasy-team-instance/:ftiId/performances
   ======================================================================================= */
app.get("/fantasy-team-instance/:ftiId/performances", async (req: Request, res: Response) => {
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

    // First verify the user has access to this fantasy team instance
    const accessCheck = await client.query(
      `
      SELECT fti.id
      FROM fantasydata.fantasy_team_instance fti
      JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
      WHERE fti.id = $1 AND ft.user_id = $2
      LIMIT 1;
      `,
      [ftiId, userId]
    );

    if (accessCheck.rowCount === 0) {
      return res.status(404).json({
        message: "Fantasy team instance not found or you do not have access to it"
      });
    }

    // Get player performances
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

          -- ✅ NEW: Player Details Added Here
          p.player_name AS name,
          p.full_name,
          p.image AS player_image,
          pos.name AS role,
          c.name AS country_name,
          c.image AS country_image,

          -- Existing Performance Data
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
          at.image AS away_team_image,

          (
            -- 1. FIELDING
            (COALESCE(ppa.catches, 0) * 8) +
            (CASE WHEN COALESCE(ppa.catches, 0) >= 3 THEN 4 ELSE 0 END) +

            -- 2. BATTING
            (COALESCE(ppa.runs_scored, 0) * 1) + 
            (COALESCE(ppa.fours, 0) * 1) +
            (COALESCE(ppa.sixes, 0) * 2) +
            (CASE WHEN COALESCE(ppa.runs_scored, 0) > 50 THEN 8 ELSE 0 END) +
            (CASE WHEN COALESCE(ppa.runs_scored, 0) > 100 THEN 8 ELSE 0 END) + 
            
            -- Batting Strike Rate Logic
            (CASE 
              WHEN COALESCE(ppa.balls_faced, 0) = 0 THEN 0 
              ELSE 
                CASE 
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

            -- Bowling Economy Logic (Runs Per Over)
            (CASE 
              WHEN COALESCE(ppa.balls_bowled, 0) = 0 THEN 0 
              ELSE 
                CASE 
                  WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) BETWEEN 0 AND 2.5 THEN 6
                  WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) > 2.5 AND (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) <= 3.49 THEN 4
                  WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) >= 3.5 AND (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) <= 4.5 THEN 2
                  WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) >= 7 AND (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) <= 8 THEN -2
                  WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) > 8 AND (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) <= 9 THEN -4
                  WHEN (COALESCE(ppa.runs_conceded, 0) * 6.0 / ppa.balls_bowled) > 9 THEN -6
                  ELSE 0
                END
            END)
          ) AS fantasy_points

      FROM player_seasons ps

      -- ✅ NEW: Joins to get Player Details
      JOIN irldata.player p ON p.id = ps.player_id
      LEFT JOIN irldata.position pos ON pos.id = p.position_id
      LEFT JOIN irldata.country_info c ON c.id = p.country_id

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

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /fantasy-team-instance/:ftiId/performances failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);

