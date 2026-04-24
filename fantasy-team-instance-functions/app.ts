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

    // Get the instance + league context in one query
    const instanceResult = await client.query(
      `
      SELECT fti.*, l.season_id, l.tournament_id
      FROM fantasydata.fantasy_team_instance fti
      JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
      JOIN fantasydata.leagues l ON l.id = ft.league_id
      WHERE fti.fantasy_team_id = $1
        AND fti.match_num = $2
        AND ft.user_id = $3
      LIMIT 1;
      `,
      [teamId, matchNum, userId]
    );

    if (instanceResult.rowCount === 0) {
      return res.status(404).json({
        message: "Fantasy team instance not found or you do not have access to it"
      });
    }

    const instance = instanceResult.rows[0];
    const { season_id, tournament_id } = instance;

    const slots = [
      'bat1', 'bat2',
      'wicket1',
      'bowl1', 'bowl2', 'bowl3',
      'all1',
      'flex1',
      'bench1', 'bench2', 'bench3'
    ];

    const playerIds = slots.map(slot => instance[slot]).filter((id: string | null) => id !== null);

    const players: Record<string, any> = {};

    if (playerIds.length > 0) {
      // For each player: get info, find their real match for this match_num, and get performance
      const playersResult = await client.query(
        `
        SELECT
          p.id,
          p.player_name,
          p.full_name,
          p.image,
          psi.role,
          p.country_id,
          c.name AS country_name,
          c.image AS country_image,

          mi.id AS match_id,
          mi.match_date,
          mi.status AS match_status,
          ht.name AS home_team_name,
          ht.image AS home_team_image,
          ht.abbreviation AS home_team_abbreviation,
          at.name AS away_team_name,
          at.image AS away_team_image,
          at.abbreviation AS away_team_abbreviation,

          pp.runs_scored,
          pp.balls_faced,
          pp.fours,
          pp.sixes,
          pp.runs_conceded,
          pp.balls_bowled,
          pp.wickets_taken,
          pp.catches,
          pp.run_outs,
          pp.catches_dropped,
          pp.not_out

        FROM irldata.player p
        LEFT JOIN irldata.country_info c ON c.id = p.country_id

        LEFT JOIN irldata.player_season_info psi
          ON psi.player_id = p.id
          AND psi.season_id = $2
          AND psi.tournament_id = $3

        LEFT JOIN irldata.match_info mi
          ON mi.season_id = $2
          AND (
            (mi.home_team_id = psi.team_id AND mi.home_match_num = $4)
            OR
            (mi.away_team_id = psi.team_id AND mi.away_match_num = $4)
          )

        LEFT JOIN irldata.team ht ON ht.id = mi.home_team_id
        LEFT JOIN irldata.team at ON at.id = mi.away_team_id

        LEFT JOIN irldata.player_performance pp
          ON pp.player_season_id = psi.id
          AND pp.match_id = mi.id

        WHERE p.id = ANY($1);
        `,
        [playerIds, season_id, tournament_id, matchNum]
      );

      const playersMap: Record<string, any> = {};
      playersResult.rows.forEach((row: any) => {
        const { id, player_name, full_name, image, role,
                country_id, country_name, country_image,
                match_id, match_date, match_status,
                home_team_name, home_team_image, away_team_name, away_team_image,
                runs_scored, balls_faced, fours, sixes,
                runs_conceded, balls_bowled, wickets_taken,
                catches, run_outs, catches_dropped } = row;

        playersMap[id] = {
          id, player_name, full_name, image, role,
          country_id, country_name, country_image,
          match: match_id ? {
            match_id,
            match_date,
            status: match_status,
            home_team_name,
            home_team_image,
            away_team_name,
            away_team_image,
          } : null,
          performance: runs_scored !== null ? {
            runs_scored, balls_faced, fours, sixes,
            runs_conceded, balls_bowled, wickets_taken,
            catches, run_outs, catches_dropped,
          } : null,
        };
      });

      slots.forEach(slot => {
        const playerId = instance[slot];
        players[slot] = (playerId && playersMap[playerId]) ? playersMap[playerId] : null;
      });
    } else {
      slots.forEach(slot => {
        players[slot] = null;
      });
    }

    return res.status(200).json({
      id: instance.id,
      fantasy_team_id: instance.fantasy_team_id,
      match_num: instance.match_num,
      captain: instance.captain,
      vice_captain: instance.vice_captain,
      is_locked: instance.is_locked,
      players
    });

  } catch (err) {
    console.error("GET /fantasy-team-instance failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET ALL FANTASY TEAM INSTANCES FOR A TEAM
   GET /fantasy-team-instance/:id/all
   ======================================================================================= */
app.get("/fantasy-team-instance/:id/all", async (req: Request, res: Response) => {
  const { id: teamId } = req.params;
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!teamId) {
    return res.status(400).json({ message: "Missing teamId parameter" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT fti.*, l.tournament_id, l.season_id
      FROM fantasydata.fantasy_team_instance fti
      JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
      JOIN fantasydata.leagues l ON l.id = ft.league_id
      WHERE fti.fantasy_team_id = $1
        AND ft.user_id = $2
      ORDER BY fti.match_num ASC;
      `,
      [teamId, userId]
    );

    const instances = result.rows;
    const tournament_id = instances[0]?.tournament_id;
    const season_id = instances[0]?.season_id;

    // Collect all unique player IDs across all instances
    const slots = [
      'bat1', 'bat2',
      'wicket1',
      'bowl1', 'bowl2', 'bowl3',
      'all1',
      'flex1',
      'bench1', 'bench2', 'bench3', 'bench4', 'bench5', 'bench6'
    ];

    const allPlayerIds = new Set<string>();
    instances.forEach((instance: any) => {
      slots.forEach(slot => {
        if (instance[slot]) {
          allPlayerIds.add(instance[slot]);
        }
      });
    });

    // Fetch all player data in one query
    let playersMap: Record<string, any> = {};
    if (allPlayerIds.size > 0) {
      const playersResult = await client.query(
        `
        SELECT
          p.id,
          p.player_name,
          p.full_name,
          p.image,
          psi.role,
          p.country_id,
          c.name AS country_name,
          c.image AS country_image
        FROM irldata.player p
        LEFT JOIN irldata.country_info c ON c.id = p.country_id
        LEFT JOIN irldata.player_season_info psi
          ON psi.player_id = p.id
         AND psi.tournament_id = $2
         AND psi.season_id = $3
        WHERE p.id = ANY($1);
        `,
        [Array.from(allPlayerIds), tournament_id, season_id]
      );

      playersResult.rows.forEach((player: any) => {
        playersMap[player.id] = player;
      });
    }

    // Build simplified response for each instance
    const response = instances.map((instance: any) => {
      const players: Record<string, any> = {};
      slots.forEach(slot => {
        const playerId = instance[slot];
        if (playerId && playersMap[playerId]) {
          players[slot] = playersMap[playerId];
        } else {
          players[slot] = null;
        }
      });
      
      return {
        id: instance.id,
        fantasy_team_id: instance.fantasy_team_id,
        match_num: instance.match_num,
        captain: instance.captain,
        vice_captain: instance.vice_captain,
        is_locked: instance.is_locked,
        players
      };
    });

    return res.status(200).json(response);

  } catch (err) {
    console.error("GET /fantasy-teams/:teamId/instances failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET FANTASY TEAM INSTANCE PERFORMANCES
   GET /fantasy-team-instance/:id/performances
   ======================================================================================= */
app.get("/fantasy-team-instance/:id/performances", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { id: ftiId } = req.params;

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
    // Get player performances
    const sql = `
      WITH fti AS (
          SELECT 
              fti.id,
              fti.fantasy_team_id,
              fti.match_num,
              ARRAY[
                  bat1, bat2,
                  wicket1,
                  bowl1, bowl2, bowl3,
                  all1, flex1,
                  bench1, bench2, bench3, bench4, bench5, bench6
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
              l.tournament_id,
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
              psi.role,
              tinfo.match_num,
              tinfo.league_id
          FROM irldata.player_season_info psi
          JOIN team_info tinfo
              ON psi.season_id = tinfo.season_id
             AND psi.tournament_id = tinfo.tournament_id
          WHERE psi.player_id = ANY(tinfo.player_ids)
      ),

      match_lookup AS (
          SELECT
              mi.id AS match_id,
              mi.season_id,
              mi.tournament_id,
              mi.home_team_id,
              mi.away_team_id,
              mi.home_match_num,
              mi.away_match_num
          FROM irldata.match_info mi
          JOIN team_info tinfo
              ON mi.season_id = tinfo.season_id
             AND mi.tournament_id = tinfo.tournament_id
      )

      SELECT 
          ps.player_season_id,
          ps.player_id,

          -- ✅ NEW: Player Details Added Here
          p.full_name AS name,
          p.full_name,
          p.image AS player_image,
          ps.role,
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
          ppa.not_out,
          ppa.inserted_at,

          ml.home_team_id,
          ht.name AS home_team_name,
          ht.image AS home_team_image,
          ht.abbreviation AS home_team_abbreviation,

          ml.away_team_id,
          at.name AS away_team_name,
          at.image AS away_team_image,
          at.abbreviation AS away_team_abbreviation,

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

      -- Player Details
      JOIN irldata.player p ON p.id = ps.player_id
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

/* =======================================================================================
   SWAP SLOTS ACROSS INSTANCES
   POST /fantasy-team-instance/:ftiId/swap-slots
   Body: { slot1: string, slot2: string }
   ======================================================================================= */
app.post("/fantasy-team-instance/:ftiId/swap-slots", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { ftiId } = req.params;
  const { slot1, slot2 } = req.body;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!ftiId) {
    return res.status(400).json({ message: "Missing fantasy team instance id" });
  }

  if (!slot1 || !slot2) {
    return res.status(400).json({ message: "Missing slot1 or slot2 in request body" });
  }

  // Validate slot names (basic format check)
  const validSlotPattern = /^(bat[1-2]|wicket1|bowl[1-3]|all1|flex1|bench[1-6])$/;
  if (!validSlotPattern.test(slot1) || !validSlotPattern.test(slot2)) {
    return res.status(400).json({ message: "Invalid slot name format" });
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

    // Call the swap_slots_across_instances function
    const result = await client.query(
      `
      SELECT fantasydata.swap_slots_across_instances($1, $2, $3) AS result;
      `,
      [ftiId, slot1, slot2]
    );

    const swapResult = result.rows[0].result;

    // Check if the swap was successful
    if (swapResult.ok) {
      return res.status(200).json(swapResult);
    } else {
      return res.status(400).json(swapResult);
    }

  } catch (err) {
    console.error("POST /fantasy-team-instances/:ftiId/swap-slots failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   UPDATE CAPTAIN AND VICE CAPTAIN
   PATCH /fantasy-team-instance/:id/captains
   Body: { captain: string, vice_captain: string }
   Updates captain/vice captain for current and all future match weeks
   Only allows update if current captain and new captain haven't played yet
   ======================================================================================= */
app.patch("/fantasy-team-instance/:id/captains", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { id: ftiId } = req.params;
  const { captain, vice_captain } = req.body;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!ftiId) {
    return res.status(400).json({ message: "Missing fantasy team instance id" });
  }

  if (!captain || !vice_captain) {
    return res.status(400).json({ message: "Missing captain or vice_captain in request body" });
  }

  if (captain === vice_captain) {
    return res.status(400).json({ message: "Captain and vice captain must be different players" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    // Get instance details, current captain, match info, and fantasy team id
    const instanceCheck = await client.query(
      `
      SELECT
        fti.id,
        fti.fantasy_team_id,
        fti.match_num,
        fti.captain AS current_captain,
        fti.vice_captain AS current_vice_captain,
        fti.is_locked,
        ft.league_id,
        l.season_id,
        ARRAY[
          fti.bat1, fti.bat2,
          fti.wicket1,
          fti.bowl1, fti.bowl2, fti.bowl3,
          fti.all1, fti.flex1
        ] AS active_players
      FROM fantasydata.fantasy_team_instance fti
      JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
      JOIN fantasydata.leagues l ON l.id = ft.league_id
      WHERE fti.id = $1 AND ft.user_id = $2
      LIMIT 1;
      `,
      [ftiId, userId]
    );

    if (instanceCheck.rowCount === 0) {
      return res.status(404).json({
        message: "Fantasy team instance not found or you do not have access to it"
      });
    }

    const instance = instanceCheck.rows[0];

    // Check if the instance is locked
    if (instance.is_locked) {
      return res.status(400).json({
        message: "Cannot update captain/vice captain - team instance is locked"
      });
    }

    // Verify that captain and vice captain are in the active roster (not bench)
    const activePlayerIds = instance.active_players.filter((id: string | null) => id !== null);

    if (!activePlayerIds.includes(captain)) {
      return res.status(400).json({
        message: "Captain must be an active player in the roster (not on bench)"
      });
    }

    if (!activePlayerIds.includes(vice_captain)) {
      return res.status(400).json({
        message: "Vice captain must be an active player in the roster (not on bench)"
      });
    }

    // Check if current captain and new captain have played in this match
    const performanceCheck = await client.query(
      `
      WITH player_check AS (
        SELECT
          psi.player_id,
          psi.team_id,
          mi.id AS match_id
        FROM irldata.player_season_info psi
        JOIN irldata.match_info mi
          ON mi.season_id = psi.season_id
          AND psi.season_id = $1
          AND (
            (psi.team_id = mi.home_team_id AND mi.home_match_num = $2)
            OR
            (psi.team_id = mi.away_team_id AND mi.away_match_num = $2)
          )
        WHERE psi.player_id = ANY($3)
      )
      SELECT
        pc.player_id,
        pp.id AS performance_id
      FROM player_check pc
      LEFT JOIN irldata.player_performance pp
        ON pp.player_season_id = (
          SELECT id FROM irldata.player_season_info
          WHERE player_id = pc.player_id AND season_id = $1
        )
        AND pp.match_id = pc.match_id
      WHERE pp.id IS NOT NULL;
      `,
      [instance.season_id, instance.match_num, [instance.current_captain, captain]]
    );

    // If any performances exist for current or new captain, deny the update
    if (performanceCheck.rowCount > 0) {
      const playedPlayerIds = performanceCheck.rows.map((row: any) => row.player_id);
      return res.status(400).json({
        message: "Cannot update captain/vice captain - one or more players have already played in this match",
        played_players: playedPlayerIds
      });
    }

    // Update captain and vice captain for this instance and all future instances
    const updateResult = await client.query(
      `
      UPDATE fantasydata.fantasy_team_instance
      SET captain = $1, vice_captain = $2
      WHERE fantasy_team_id = $3
        AND match_num >= $4
      RETURNING id, match_num, captain, vice_captain;
      `,
      [captain, vice_captain, instance.fantasy_team_id, instance.match_num]
    );

    return res.status(200).json({
      message: "Captain and vice captain updated successfully for current and future weeks",
      updated_count: updateResult.rowCount,
      instances: updateResult.rows
    });

  } catch (err) {
    console.error("PATCH /fantasy-team-instance/:id/captains failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);

