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
    ) AS teams

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
        SELECT
            l.id,
            l.name,
            l.tournament_id,
            l.creator_id,
            l.status,
            l.max_teams,
            l.join_code,
            l.season_id
        FROM fantasydata.leagues l
        JOIN fantasydata.fantasy_teams ft ON ft.league_id = l.id
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
        max_teams || 10,
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
        s.end_year AS season_year,
        (
            SELECT json_agg(
                to_jsonb(all_ft) || jsonb_build_object('user_name', p.full_name)
            )
            FROM fantasydata.fantasy_teams all_ft
            JOIN authdata.profiles p ON p.user_id = all_ft.user_id
            WHERE all_ft.league_id = l.id
        ) AS teams
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
