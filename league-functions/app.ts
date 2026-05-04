import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
GET ALL LIVE / UPCOMING MATCHES FOR USER (HOME FEED)
GET /matches/feed
======================================================================================= */
app.get("/leagues", async (req: Request, res: Response) => {

  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return {
      statusCode: 401,
      body: JSON.stringify({ message: "Unauthorized" })
    };
  }

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();
    const result = await client.query(
      `
      SELECT
    l.id,
    l.name,
    l.tournament_id,
    l.creator_id,
    l.status,
    l.max_teams,
    l.join_code,
    l.season_id,

	ft.id as user_team_id,
    ti.abbreviation AS tournament_abbr,

    ti.weeks AS total_games,
    s.end_year AS season_year,

    (
        SELECT MAX(fm.match_num)
        FROM fantasydata.fantasy_team_instance fti
        JOIN fantasydata.fantasy_matchups fm
            ON fm.fantasy_team_instance1_id = fti.id
            OR fm.fantasy_team_instance2_id = fti.id
        WHERE fti.fantasy_team_id = ft.id
    ) AS latest_game,
    (
        SELECT json_agg(
            to_jsonb(all_ft) || jsonb_build_object('user_name', p.full_name)
        )
        FROM fantasydata.fantasy_teams all_ft
        JOIN authdata.profiles p ON p.user_id = all_ft.user_id
        WHERE all_ft.league_id = l.id
    ) AS teams,
    (
        SELECT jsonb_build_object(
            'time_per_pick', lds.time_per_pick,
            'pick_warning_seconds', lds.pick_warning_seconds,
            'snake_draft', lds.snake_draft
        )
        FROM fantasydata.league_draft_settings lds
        WHERE lds.league_id = l.id
    ) AS draft_settings,
    (
        SELECT json_agg(row_to_json(lsr.*) ORDER BY lsr.category, lsr.stat)
        FROM fantasydata.league_scoring_rules lsr
        WHERE lsr.league_id = l.id
    ) AS scoring_rules

FROM fantasydata.leagues l
JOIN fantasydata.fantasy_teams ft
    ON ft.league_id = l.id
    AND ft.user_id = $1
JOIN irldata.tournament_info ti
    ON ti.id = l.tournament_id
JOIN irldata.season s
    ON s.id = l.season_id

ORDER BY l.name ASC;
      `,
      [userId]
    );
    return res.status(200).json(result.rows);
  } catch (err) {
    console.error("GET /leagues failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET LEAGUE BY JOIN CODE
   GET /leagues/join/:joinCode
   ======================================================================================= */
app.get("/leagues/join/:joinCode", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  const { joinCode } = req.params;
  if (!joinCode) {
    return res.status(400).json({ message: "Join code is required" });
  }

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      SELECT
        l.id,
        l.name,
        l.tournament_id,
        l.creator_id,
        l.status,
        l.max_teams,
        l.join_code,
        l.season_id,
        ti.abbreviation AS tournament_abbr,
        ti.weeks AS total_games,
        s.end_year AS season_year,
        (
          SELECT json_agg(
            to_jsonb(all_ft) || jsonb_build_object('user_name', p.full_name)
          )
          FROM fantasydata.fantasy_teams all_ft
          JOIN authdata.profiles p ON p.user_id = all_ft.user_id
          WHERE all_ft.league_id = l.id
        ) AS teams,
        (
          SELECT jsonb_build_object(
            'time_per_pick', lds.time_per_pick,
            'pick_warning_seconds', lds.pick_warning_seconds,
            'snake_draft', lds.snake_draft
          )
          FROM fantasydata.league_draft_settings lds
          WHERE lds.league_id = l.id
        ) AS draft_settings,
        (
          SELECT json_agg(row_to_json(lsr.*) ORDER BY lsr.category, lsr.stat)
          FROM fantasydata.league_scoring_rules lsr
          WHERE lsr.league_id = l.id
        ) AS scoring_rules
      FROM fantasydata.leagues l
      JOIN irldata.tournament_info ti ON ti.id = l.tournament_id
      JOIN irldata.season s ON s.id = l.season_id
      WHERE l.join_code = $1 AND l.status = 'draft_pending'
    `;

    const result = await client.query(sql, [joinCode]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "League not found or not available to join" });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /leagues/join/:joinCode failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   JOIN LEAGUE BY JOIN CODE
   POST /leagues/join/:joinCode
   ======================================================================================= */
app.post("/leagues/join/:joinCode", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  const { joinCode } = req.params;
  if (!joinCode) {
    return res.status(400).json({ message: "Join code is required" });
  }

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  const { team_name, team_color, abbreviation, team_icon } = req.body;

  if (!team_name || !team_color || !abbreviation) {
    return res.status(400).json({ message: "Missing required fields: team_name, team_color, abbreviation" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    // Get league by join code
    const leagueResult = await client.query(
      `SELECT id, max_teams, status FROM fantasydata.leagues WHERE join_code = $1`,
      [joinCode]
    );

    if (leagueResult.rowCount === 0) {
      return res.status(404).json({ message: "League not found" });
    }

    const league = leagueResult.rows[0];

    if (league.status !== "draft_pending") {
      return res.status(400).json({ message: "League is not available to join" });
    }

    // Check if user already has a team in this league
    const existingTeamResult = await client.query(
      `SELECT id FROM fantasydata.fantasy_teams WHERE league_id = $1 AND user_id = $2`,
      [league.id, userId]
    );

    if (existingTeamResult.rowCount && existingTeamResult.rowCount > 0) {
      return res.status(409).json({ message: "You already have a team in this league" });
    }

    // Check if league is at max capacity
    const teamCountResult = await client.query(
      `SELECT COUNT(*) as team_count FROM fantasydata.fantasy_teams WHERE league_id = $1`,
      [league.id]
    );

    const currentTeamCount = parseInt(teamCountResult.rows[0].team_count, 10);

    if (currentTeamCount >= league.max_teams) {
      return res.status(400).json({ message: "League is at max capacity" });
    }

    // Create the fantasy team
    const insertResult = await client.query(
      `INSERT INTO fantasydata.fantasy_teams (league_id, user_id, team_name, team_color, abbreviation, team_icon)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id`,
      [league.id, userId, team_name, team_color, abbreviation, team_icon ?? null]
    );

    const newTeamId = insertResult.rows[0].id;

    // Fetch the league with all details to return
    const fullLeagueResult = await client.query(
      `
      SELECT
        l.id,
        l.name,
        l.tournament_id,
        l.creator_id,
        l.status,
        l.max_teams,
        l.join_code,
        l.season_id,
        $1::uuid as user_team_id,
        ti.abbreviation AS tournament_abbr,
        ti.weeks AS total_games,
        s.end_year AS season_year,
        (
          SELECT json_agg(
            to_jsonb(all_ft) || jsonb_build_object('user_name', p.full_name)
          )
          FROM fantasydata.fantasy_teams all_ft
          JOIN authdata.profiles p ON p.user_id = all_ft.user_id
          WHERE all_ft.league_id = l.id
        ) AS teams,
        (
          SELECT jsonb_build_object(
            'time_per_pick', lds.time_per_pick,
            'pick_warning_seconds', lds.pick_warning_seconds,
            'snake_draft', lds.snake_draft
          )
          FROM fantasydata.league_draft_settings lds
          WHERE lds.league_id = l.id
        ) AS draft_settings,
        (
          SELECT json_agg(row_to_json(lsr.*) ORDER BY lsr.category, lsr.stat)
          FROM fantasydata.league_scoring_rules lsr
          WHERE lsr.league_id = l.id
        ) AS scoring_rules
      FROM fantasydata.leagues l
      JOIN irldata.tournament_info ti ON ti.id = l.tournament_id
      JOIN irldata.season s ON s.id = l.season_id
      WHERE l.id = $2
      `,
      [newTeamId, league.id]
    );

    return res.status(201).json(fullLeagueResult.rows[0]);

  } catch (err) {
    console.error("POST /leagues/join/:joinCode failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET DEFAULT SCORING RULES
   GET /leagues/scoring-rules
   ======================================================================================= */
app.get("/leagues/scoring-rules", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
        SELECT
            lsr.id,
            lsr.league_id,
            lsr.stat,
            lsr.category,
            lsr.mode,
            lsr.per_unit_points,
            lsr.flat_points,
            lsr.threshold,
            lsr.band,
            lsr.multiplier,
            lsr.created_at
        FROM fantasydata.league_scoring_rules lsr
        WHERE lsr.league_id IS NULL
        ORDER BY lsr.category, lsr.stat;
    `;

    const result = await client.query(sql);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "No Default Scoring Rules Found" });
    }

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /leagues/scoring-rules failed", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET SPECIFIC LEAGUE DETAILS
   GET /leagues/:leagueId
   ======================================================================================= */
app.get("/leagues/:leagueId", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { leagueId } = req.params;
  if (!leagueId) {
    return res.status(400).json({ message: "League Id is required" });
  }

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }


  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
        WITH
        target_league AS (
            SELECT l.id, l.season_id, l.tournament_id
            FROM fantasydata.leagues l
            WHERE l.id = $2
        ),

        completed_matchups AS (
            SELECT
                fm.id AS matchup_id,
                fm.match_num,
                fm.fantasy_team_instance1_id,
                fm.fantasy_team_instance2_id,
                fm.fantasy_winner_team_instance_id,
                tl.season_id,
                tl.tournament_id
            FROM fantasydata.fantasy_matchups fm
            CROSS JOIN target_league tl
            WHERE fm.league_id = tl.id
              AND fm.fantasy_winner_team_instance_id IS NOT NULL
        ),

        league_rules AS (
            SELECT lsr.*
            FROM fantasydata.league_scoring_rules lsr
            WHERE lsr.league_id = $2
        ),

        team_rosters AS (
            SELECT
                cm.matchup_id, cm.match_num, cm.season_id, cm.tournament_id,
                side.team_side, side.instance_id,
                ti.captain, ti.vice_captain,
                u.player_id
            FROM completed_matchups cm
            CROSS JOIN LATERAL (VALUES
                (1, cm.fantasy_team_instance1_id),
                (2, cm.fantasy_team_instance2_id)
            ) AS side(team_side, instance_id)
            JOIN fantasydata.fantasy_team_instance ti ON ti.id = side.instance_id
            CROSS JOIN LATERAL (VALUES
                (ti.bat1),(ti.bat2),(ti.wicket1),
                (ti.bowl1),(ti.bowl2),(ti.bowl3),
                (ti.all1),(ti.flex1)
            ) AS u(player_id)
            WHERE u.player_id IS NOT NULL
        ),

        resolved_performances AS (
            SELECT
                tr.matchup_id, tr.team_side, tr.instance_id,
                tr.player_id, tr.captain, tr.vice_captain,
                pp.runs_scored, pp.balls_faced, pp.fours, pp.sixes,
                pp.balls_bowled, pp.runs_conceded, pp.wickets_taken,
                pp.catches, pp.run_outs, pp.catches_dropped, pp.not_out
            FROM team_rosters tr
            JOIN irldata.player_season_info psi
                ON psi.player_id = tr.player_id
               AND psi.season_id = tr.season_id
               AND psi.tournament_id = tr.tournament_id
            JOIN irldata.match_info mi
                ON mi.season_id = tr.season_id
               AND mi.tournament_id = tr.tournament_id
               AND (
                    (mi.home_team_id = psi.team_id AND mi.home_match_num = tr.match_num)
                    OR (mi.away_team_id = psi.team_id AND mi.away_match_num = tr.match_num)
               )
            JOIN irldata.player_performance pp
                ON pp.match_id = mi.id
               AND pp.player_season_id = psi.id
        ),

        player_stats_calc AS (
            SELECT rp.*,
                CASE WHEN COALESCE(rp.balls_faced, 0) > 0
                     THEN (rp.runs_scored * 100.0 / rp.balls_faced)::NUMERIC ELSE 0 END AS strike_rate,
                CASE WHEN COALESCE(rp.balls_bowled, 0) > 0
                     THEN (rp.runs_conceded / (rp.balls_bowled / 6.0))::NUMERIC ELSE 0 END AS economy
            FROM resolved_performances rp
        ),

        standard_points AS (
            SELECT ps.matchup_id, ps.instance_id, ps.player_id,
                SUM(CASE
                    WHEN r.stat = 'Points per run' THEN COALESCE(ps.runs_scored, 0) * r.per_unit_points
                    WHEN r.stat = 'Bonus per 4' THEN COALESCE(ps.fours, 0) * r.per_unit_points
                    WHEN r.stat = 'Bonus per 6' THEN COALESCE(ps.sixes, 0) * r.per_unit_points
                    WHEN r.stat = 'Bonus per half-century' AND COALESCE(ps.runs_scored, 0) >= 50 THEN r.flat_points
                    WHEN r.stat = 'Bonus per century' AND COALESCE(ps.runs_scored, 0) >= 100 THEN r.flat_points
                    WHEN r.stat = 'Duck-out Penalty' AND COALESCE(ps.runs_scored, 0) = 0
                         AND COALESCE(ps.balls_faced, 0) > 0 THEN r.flat_points
                    WHEN r.stat = 'Points per Wicket' THEN COALESCE(ps.wickets_taken, 0) * r.per_unit_points
                    WHEN r.stat = '3-Wicket Bonus' THEN FLOOR(COALESCE(ps.wickets_taken, 0) / 3.0) * r.per_unit_points
                    WHEN r.stat = '5-Wicket Bonus' THEN FLOOR(COALESCE(ps.wickets_taken, 0) / 5.0) * r.per_unit_points
                    WHEN r.stat = 'Points per catch' THEN COALESCE(ps.catches, 0) * r.per_unit_points
                    WHEN r.stat = '3-Catches bonus' THEN FLOOR(COALESCE(ps.catches, 0) / 3.0) * r.per_unit_points
                    WHEN r.stat = 'Run Out' THEN COALESCE(ps.run_outs, 0) * r.per_unit_points
                    WHEN r.stat = 'Dropped Catch' THEN COALESCE(ps.catches_dropped, 0) * r.per_unit_points
                    ELSE 0
                END) AS total_std_points
            FROM player_stats_calc ps
            CROSS JOIN league_rules r
            WHERE r.mode != 'band' AND r.category != 'leadership'
            GROUP BY ps.matchup_id, ps.instance_id, ps.player_id
        ),

        band_points AS (
            SELECT ps.matchup_id, ps.instance_id, ps.player_id,
                SUM(r.flat_points) AS total_band_points
            FROM player_stats_calc ps
            JOIN league_rules r ON r.mode = 'band'
            WHERE
                (r.stat = 'Strike Rate' AND COALESCE(ps.balls_faced, 0) > 0 AND r.band @> ps.strike_rate)
                OR
                (r.stat = 'Economy' AND COALESCE(ps.balls_bowled, 0) > 0 AND r.band @> ps.economy)
            GROUP BY ps.matchup_id, ps.instance_id, ps.player_id
        ),

        individual_scores AS (
            SELECT
                ps.matchup_id, ps.team_side, ps.instance_id, ps.player_id,
                (COALESCE(sp.total_std_points, 0) + COALESCE(bp.total_band_points, 0))
                * COALESCE((
                    SELECT multiplier FROM league_rules
                    WHERE stat = 'Captaincy Multiplier'
                      AND ps.player_id = ps.captain
                ), 1)
                * COALESCE((
                    SELECT multiplier FROM league_rules
                    WHERE stat = 'Vice Captaincy Multiplier'
                      AND ps.player_id = ps.vice_captain
                ), 1) AS final_player_score
            FROM player_stats_calc ps
            LEFT JOIN standard_points sp
                ON sp.matchup_id = ps.matchup_id
               AND sp.instance_id = ps.instance_id
               AND sp.player_id = ps.player_id
            LEFT JOIN band_points bp
                ON bp.matchup_id = ps.matchup_id
               AND bp.instance_id = ps.instance_id
               AND bp.player_id = ps.player_id
        ),

        matchup_scores AS (
            SELECT
                cm.matchup_id,
                cm.match_num,
                cm.fantasy_team_instance1_id,
                cm.fantasy_team_instance2_id,
                cm.fantasy_winner_team_instance_id,
                COALESCE(SUM(CASE WHEN ind.team_side = 1 THEN ind.final_player_score ELSE 0 END), 0) AS team1_score,
                COALESCE(SUM(CASE WHEN ind.team_side = 2 THEN ind.final_player_score ELSE 0 END), 0) AS team2_score
            FROM completed_matchups cm
            LEFT JOIN individual_scores ind ON ind.matchup_id = cm.matchup_id
            GROUP BY cm.matchup_id, cm.match_num,
                     cm.fantasy_team_instance1_id, cm.fantasy_team_instance2_id,
                     cm.fantasy_winner_team_instance_id
        ),

        instance_matchup_scores AS (
            SELECT
                matchup_id, match_num,
                fantasy_team_instance1_id AS instance_id,
                team1_score AS score,
                (fantasy_winner_team_instance_id = fantasy_team_instance1_id) AS won
            FROM matchup_scores
            UNION ALL
            SELECT
                matchup_id, match_num,
                fantasy_team_instance2_id AS instance_id,
                team2_score AS score,
                (fantasy_winner_team_instance_id = fantasy_team_instance2_id) AS won
            FROM matchup_scores
        ),

        team_stats AS (
            SELECT
                fti.fantasy_team_id,
                COUNT(*) FILTER (WHERE ims.won)::int AS wins,
                COUNT(*) FILTER (WHERE NOT ims.won)::int AS losses,
                COUNT(*)::int AS matches_completed,
                AVG(ims.score)::NUMERIC AS avg_points_per_game,
                json_agg(
                    jsonb_build_object('match_num', ims.match_num, 'points', ims.score)
                    ORDER BY ims.match_num
                ) AS match_scores
            FROM instance_matchup_scores ims
            JOIN fantasydata.fantasy_team_instance fti ON fti.id = ims.instance_id
            GROUP BY fti.fantasy_team_id
        )

        SELECT
            l.id,
            l.name,
            l.tournament_id,
            l.creator_id,
            l.status,
            l.max_teams,
            l.join_code,
            l.season_id,
            ti.weeks AS total_games,
            (
                SELECT json_agg(
                    to_jsonb(all_ft) || jsonb_build_object(
                        'user_name', p.full_name,
                        'wins', COALESCE(ts.wins, 0),
                        'losses', COALESCE(ts.losses, 0),
                        'matches_completed', COALESCE(ts.matches_completed, 0),
                        'avg_points_per_game', COALESCE(ts.avg_points_per_game, 0),
                        'match_scores', COALESCE(ts.match_scores, '[]'::json)
                    )
                )
                FROM fantasydata.fantasy_teams all_ft
                JOIN authdata.profiles p ON p.user_id = all_ft.user_id
                LEFT JOIN team_stats ts ON ts.fantasy_team_id = all_ft.id
                WHERE all_ft.league_id = l.id
            ) AS teams,
            (
                SELECT jsonb_build_object(
                    'time_per_pick', lds.time_per_pick,
                    'pick_warning_seconds', lds.pick_warning_seconds,
                    'snake_draft', lds.snake_draft
                )
                FROM fantasydata.league_draft_settings lds
                WHERE lds.league_id = l.id
            ) AS draft_settings,
            (
                SELECT json_agg(row_to_json(lsr.*) ORDER BY lsr.category, lsr.stat)
                FROM fantasydata.league_scoring_rules lsr
                WHERE lsr.league_id = l.id
            ) AS scoring_rules
        FROM fantasydata.leagues l
        JOIN fantasydata.fantasy_teams ft ON ft.league_id = l.id
        JOIN irldata.tournament_info ti ON ti.id = l.tournament_id
        WHERE ft.user_id = $1
          AND l.id = $2;
    `;

    const result = await client.query(sql, [userId, leagueId]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "Match not found or you do not have access" });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /matches/:matchId failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   UPDATE DRAFT SETTINGS FOR A LEAGUE
   PUT /leagues/:leagueId/draft-settings
   ======================================================================================= */
app.put("/leagues/:leagueId/draft-settings", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  const { leagueId } = req.params;
  if (!leagueId) {
    return res.status(400).json({ message: "League Id is required" });
  }

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  const { time_per_pick, pick_warning_seconds, snake_draft } = req.body;

  if (time_per_pick === undefined && pick_warning_seconds === undefined && snake_draft === undefined) {
    return res.status(400).json({ message: "At least one draft setting field is required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    // Verify user is the league creator
    const leagueCheck = await client.query(
      `SELECT creator_id FROM fantasydata.leagues WHERE id = $1`,
      [leagueId]
    );

    if (leagueCheck.rowCount === 0) {
      return res.status(404).json({ message: "League not found" });
    }

    if (leagueCheck.rows[0].creator_id !== userId) {
      return res.status(403).json({ message: "Only the league creator can update draft settings" });
    }

    // Build dynamic update query
    const updates: string[] = [];
    const values: (number | boolean | string)[] = [];
    let paramIndex = 1;

    if (time_per_pick !== undefined) {
      updates.push(`time_per_pick = $${paramIndex++}`);
      values.push(time_per_pick);
    }
    if (pick_warning_seconds !== undefined) {
      updates.push(`pick_warning_seconds = $${paramIndex++}`);
      values.push(pick_warning_seconds);
    }
    if (snake_draft !== undefined) {
      updates.push(`snake_draft = $${paramIndex++}`);
      values.push(snake_draft);
    }

    values.push(leagueId);

    const sql = `
      UPDATE fantasydata.league_draft_settings
      SET ${updates.join(", ")}
      WHERE league_id = $${paramIndex}
      RETURNING time_per_pick, pick_warning_seconds, snake_draft
    `;

    const result = await client.query(sql, values);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "Draft settings not found for this league" });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("PUT /leagues/:leagueId/draft-settings failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

app.get("/leagues/:leagueId/scoring-rules", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  const { leagueId } = req.params;
  if (!leagueId) {
    return res.status(400).json({ message: "League Id is required" });
  }


  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
        SELECT
            lsr.id,
            lsr.league_id,
            lsr.stat,
            lsr.category,
            lsr.mode,
            lsr.per_unit_points,
            lsr.flat_points,
            lsr.threshold,
            lsr.band,
            lsr.multiplier,
            lsr.created_at
        FROM fantasydata.league_scoring_rules lsr
        WHERE lsr.league_id = $1
        ORDER BY lsr.category, lsr.stat;
    `;

    const result = await client.query(sql, [leagueId]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "No Default Scoring Rules Found" });
    }

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /leagues/{leagueId}/scoring-rules failed", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   UPDATE SCORING RULES FOR A LEAGUE
   PUT /leagues/:leagueId/scoring-rules
   ======================================================================================= */
app.put("/leagues/:leagueId/scoring-rules", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  const { leagueId } = req.params;
  if (!leagueId) {
    return res.status(400).json({ message: "League Id is required" });
  }

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  const { scoring_rules } = req.body;

  if (!scoring_rules || !Array.isArray(scoring_rules) || scoring_rules.length === 0) {
    return res.status(400).json({ message: "scoring_rules array is required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    // Verify user is the league creator
    const leagueCheck = await client.query(
      `SELECT creator_id FROM fantasydata.leagues WHERE id = $1`,
      [leagueId]
    );

    if (leagueCheck.rowCount === 0) {
      return res.status(404).json({ message: "League not found" });
    }

    if (leagueCheck.rows[0].creator_id !== userId) {
      return res.status(403).json({ message: "Only the league creator can update scoring rules" });
    }

    await client.query("BEGIN");

    // Update existing scoring rules by ID
    const updateSql = `
      UPDATE fantasydata.league_scoring_rules
      SET stat = $2,
          category = $3,
          mode = $4,
          per_unit_points = $5,
          flat_points = $6,
          threshold = $7,
          band = $8,
          multiplier = $9
      WHERE id = $1 AND league_id = $10
    `;

    for (const rule of scoring_rules) {
      await client.query(updateSql, [
        rule.id,
        rule.stat,
        rule.category,
        rule.mode,
        rule.per_unit_points ?? null,
        rule.flat_points ?? null,
        rule.threshold ?? null,
        rule.band ?? null,
        rule.multiplier ?? null,
        leagueId
      ]);
    }

    await client.query("COMMIT");

    // Fetch and return the updated scoring rules
    const result = await client.query(
      `SELECT id, league_id, stat, category, mode, per_unit_points, flat_points, threshold, band, multiplier, created_at
       FROM fantasydata.league_scoring_rules
       WHERE league_id = $1
       ORDER BY category, stat`,
      [leagueId]
    );

    return res.status(200).json(result.rows);

  } catch (err) {
    if (client) {
      await client.query("ROLLBACK");
    }
    console.error("PUT /leagues/:leagueId/scoring-rules failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   SET DRAFT ORDER AND START DRAFT
   PUT /leagues/:leagueId/draft-order
   ======================================================================================= */
app.put("/leagues/:leagueId/draft-order", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  const { leagueId } = req.params;
  if (!leagueId) {
    return res.status(400).json({ message: "League Id is required" });
  }

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  const { draft_order } = req.body;

  if (!draft_order || !Array.isArray(draft_order)) {
    return res.status(400).json({ message: "draft_order array is required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    // Verify league exists and user is the creator
    const leagueCheck = await client.query(
      `SELECT creator_id, status FROM fantasydata.leagues WHERE id = $1`,
      [leagueId]
    );

    if (leagueCheck.rowCount === 0) {
      return res.status(404).json({ message: "League not found" });
    }

    const league = leagueCheck.rows[0];

    if (league.creator_id !== userId) {
      return res.status(403).json({ message: "Only the league creator can set draft order" });
    }

    if (league.status !== "draft_pending") {
      return res.status(400).json({ message: "Draft can only be started when league is in draft_pending status" });
    }

    await client.query("BEGIN");

    // Update draft_order for specified teams
    const specifiedTeamIds: string[] = [];
    let maxSpecifiedOrder = 0;

    for (const entry of draft_order) {
      if (!entry.team_id || typeof entry.order !== "number") {
        await client.query("ROLLBACK");
        return res.status(400).json({ message: "Each draft_order entry must have team_id and order" });
      }

      await client.query(
        `UPDATE fantasydata.fantasy_teams SET draft_order = $1 WHERE id = $2 AND league_id = $3`,
        [entry.order, entry.team_id, leagueId]
      );

      specifiedTeamIds.push(entry.team_id);
      if (entry.order > maxSpecifiedOrder) {
        maxSpecifiedOrder = entry.order;
      }
    }

    // Get unspecified teams ordered by created_at
    const unspecifiedTeamsResult = await client.query(
      `SELECT id FROM fantasydata.fantasy_teams
       WHERE league_id = $1 AND id != ALL($2::uuid[])
       ORDER BY created_at ASC`,
      [leagueId, specifiedTeamIds]
    );

    // Assign sequential draft orders to unspecified teams
    let nextOrder = maxSpecifiedOrder + 1;
    for (const team of unspecifiedTeamsResult.rows) {
      await client.query(
        `UPDATE fantasydata.fantasy_teams SET draft_order = $1 WHERE id = $2`,
        [nextOrder, team.id]
      );
      nextOrder++;
    }

    // Update league status to draft_in_progress
    await client.query(
      `UPDATE fantasydata.leagues SET status = 'draft_in_progress' WHERE id = $1`,
      [leagueId]
    );

    await client.query("COMMIT");

    // Fetch and return updated league details
    const result = await client.query(
      `
      SELECT
        l.id,
        l.name,
        l.tournament_id,
        l.creator_id,
        l.status,
        l.max_teams,
        l.join_code,
        l.season_id,
        ti.abbreviation AS tournament_abbr,
        ti.weeks AS total_games,
        s.end_year AS season_year,
        (
          SELECT json_agg(
            to_jsonb(all_ft) || jsonb_build_object('user_name', p.full_name)
            ORDER BY all_ft.draft_order
          )
          FROM fantasydata.fantasy_teams all_ft
          JOIN authdata.profiles p ON p.user_id = all_ft.user_id
          WHERE all_ft.league_id = l.id
        ) AS teams,
        (
          SELECT jsonb_build_object(
            'time_per_pick', lds.time_per_pick,
            'pick_warning_seconds', lds.pick_warning_seconds,
            'snake_draft', lds.snake_draft
          )
          FROM fantasydata.league_draft_settings lds
          WHERE lds.league_id = l.id
        ) AS draft_settings,
        (
          SELECT json_agg(row_to_json(lsr.*) ORDER BY lsr.category, lsr.stat)
          FROM fantasydata.league_scoring_rules lsr
          WHERE lsr.league_id = l.id
        ) AS scoring_rules
      FROM fantasydata.leagues l
      JOIN irldata.tournament_info ti ON ti.id = l.tournament_id
      JOIN irldata.season s ON s.id = l.season_id
      WHERE l.id = $1
      `,
      [leagueId]
    );

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    if (client) {
      await client.query("ROLLBACK");
    }
    console.error("PUT /leagues/:leagueId/draft-order failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET POSITION RULES FOR A LEAGUE
   GET /leagues/:leagueId/position-rules
   ======================================================================================= */
app.get("/leagues/:leagueId/position-rules", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  const { leagueId } = req.params;
  if (!leagueId) {
    return res.status(400).json({ message: "League Id is required" });
  }

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    // Verify user has access to this league
    const accessCheck = await client.query(
      `SELECT 1 FROM fantasydata.fantasy_teams WHERE league_id = $1 AND user_id = $2`,
      [leagueId, userId]
    );

    if (accessCheck.rowCount === 0) {
      return res.status(403).json({ message: "You do not have access to this league" });
    }

    const sql = `
      SELECT
        lpr.id,
        lpr.league_id,
        lpr.min_count,
        lpr.max_count,
        lpr.roles,
        (
          SELECT json_agg(
            jsonb_build_object(
              'id', p.id,
              'name', p.name
            )
          )
          FROM irldata.position p
          WHERE p.id = ANY(lpr.roles)
        ) AS role_details
      FROM fantasydata.league_position_rules lpr
      WHERE lpr.league_id = $1;
    `;

    const result = await client.query(sql, [leagueId]);

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /leagues/:leagueId/position-rules failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   CREATE LEAGUE
   POST /leagues
   ======================================================================================= */
app.post("/leagues", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  const { name, tournament_id, max_teams, scoring_rules, team } = req.body;

  if (!name || !tournament_id) {
    return res.status(400).json({ message: "Missing required fields: name, tournament_id" });
  }

  if (!team?.name || !team?.color || !team?.abbreviation) {
    return res.status(400).json({ message: "Missing required fields: team.name, team.color, team.abbreviation" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    // Look up tournament's default max_teams to use as fallback
    const tournamentResult = await client.query(
      `SELECT max_teams FROM irldata.tournament_info WHERE id = $1`,
      [tournament_id]
    );

    if (tournamentResult.rowCount === 0) {
      return res.status(404).json({ message: "Tournament not found" });
    }

    const resolvedMaxTeams = max_teams || tournamentResult.rows[0].max_teams;

    // Call the stored procedure to create league with rules, using latest season via subquery
    const result = await client.query(
      `SELECT fantasydata.create_league_with_rules(
        jsonb_build_object(
          'name', $1::text,
          'tournament_id', $2::uuid,
          'season_id', (SELECT id FROM irldata.season WHERE tournament_id = $2 ORDER BY end_year DESC LIMIT 1),
          'max_teams', $3::int,
          'status', 'draft_pending'
        ),
        $4::jsonb,
        $5::text,
        $6::jsonb
      ) as league_id`,
      [
        name,
        tournament_id,
        resolvedMaxTeams,
        scoring_rules ? JSON.stringify(scoring_rules) : null,
        userId,
        JSON.stringify({ team_name: team.name, team_color: team.color, abbreviation: team.abbreviation, team_icon: team.icon ?? null })
      ]
    );

    const leagueId = result.rows[0].league_id;

    // Fetch the created league with all details
    const leagueResult = await client.query(
      `
      SELECT
        l.id,
        l.name,
        l.tournament_id,
        l.creator_id,
        l.status,
        l.max_teams,
        l.join_code,
        l.season_id,
        ft.id as user_team_id,
        ti.abbreviation AS tournament_abbr,
        ti.weeks AS total_games,
        s.end_year AS season_year,
        (
            SELECT json_agg(
                to_jsonb(all_ft) || jsonb_build_object('user_name', p.full_name)
            )
            FROM fantasydata.fantasy_teams all_ft
            JOIN authdata.profiles p ON p.user_id = all_ft.user_id
            WHERE all_ft.league_id = l.id
        ) AS teams,
        (
            SELECT jsonb_build_object(
                'time_per_pick', lds.time_per_pick,
                'pick_warning_seconds', lds.pick_warning_seconds,
                'snake_draft', lds.snake_draft
            )
            FROM fantasydata.league_draft_settings lds
            WHERE lds.league_id = l.id
        ) AS draft_settings,
        (
            SELECT json_agg(row_to_json(lsr.*) ORDER BY lsr.category, lsr.stat)
            FROM fantasydata.league_scoring_rules lsr
            WHERE lsr.league_id = l.id
        ) AS scoring_rules
      FROM fantasydata.leagues l
      JOIN fantasydata.fantasy_teams ft
          ON ft.league_id = l.id
          AND ft.user_id = $1
      JOIN irldata.tournament_info ti
          ON ti.id = l.tournament_id
      JOIN irldata.season s
          ON s.id = l.season_id
      WHERE l.id = $2
      `,
      [userId, leagueId]
    );

    return res.status(201).json(leagueResult.rows[0]);

  } catch (err) {
    console.error("POST /leagues failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred", error: String(err) });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
