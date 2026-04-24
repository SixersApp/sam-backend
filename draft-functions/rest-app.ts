import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";


const app = createApp();

/* =======================================================================================
   GET PAGINATED PLAYERS FOR DRAFT
   GET /draft/players?leagueId=...&limit=50&offset=0
   ======================================================================================= */
app.get("/draft/players", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  const { leagueId, limit: limitParam, offset: offsetParam } = req.query;

  if (!leagueId) {
    return res.status(400).json({ message: "leagueId is required" });
  }

  const limit = Math.min(Math.max(parseInt(limitParam as string, 10) || 50, 1), 100);
  const offset = Math.max(parseInt(offsetParam as string, 10) || 0, 0);

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    // Verify user is a member of the league
    const memberCheck = await client.query(
      `SELECT 1 FROM fantasydata.fantasy_teams WHERE league_id = $1 AND user_id = $2`,
      [leagueId, userId]
    );

    if ((memberCheck.rowCount ?? 0) === 0) {
      return res.status(403).json({ message: "You are not a member of this league" });
    }

    // Get season_id and tournament_id from the league
    const leagueResult = await client.query(
      `SELECT season_id, tournament_id FROM fantasydata.leagues WHERE id = $1`,
      [leagueId]
    );

    if ((leagueResult.rowCount ?? 0) === 0) {
      return res.status(404).json({ message: "League not found" });
    }

    const { season_id, tournament_id } = leagueResult.rows[0];

    // Query players sorted by rank with pagination
    const playersResult = await client.query(
      `SELECT
        p.id,
        p.player_name,
        p.full_name,
        p.image,
        psi.role,
        ci.name AS country_name,
        ci.image AS country_image,
        psi.rank,
        psi.initial_projection,
        t.name AS team_name,
        t.abbreviation AS team_abbreviation
      FROM irldata.player_season_info psi
      JOIN irldata.player p ON p.id = psi.player_id
      LEFT JOIN irldata.country_info ci ON ci.id = p.country_id
      LEFT JOIN irldata.team t ON t.id = psi.team_id
      WHERE psi.season_id = $1
        AND psi.tournament_id = $2
      ORDER BY psi.rank ASC
      LIMIT $3 OFFSET $4`,
      [season_id, tournament_id, limit, offset]
    );

    return res.status(200).json(playersResult.rows);
  } catch (err) {
    console.error("GET /draft/players failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET PAGINATED PLAYERS FOR TRANSACTIONS (WITH AVAILABILITY)
   GET /draft/players/available?leagueId=...&limit=50&offset=0
   ======================================================================================= */
app.get("/draft/players/available", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  const { leagueId, limit: limitParam, offset: offsetParam } = req.query;

  if (!leagueId) {
    return res.status(400).json({ message: "leagueId is required" });
  }

  const limit = Math.min(Math.max(parseInt(limitParam as string, 10) || 50, 1), 100);
  const offset = Math.max(parseInt(offsetParam as string, 10) || 0, 0);

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    const memberCheck = await client.query(
      `SELECT 1 FROM fantasydata.fantasy_teams WHERE league_id = $1 AND user_id = $2`,
      [leagueId, userId]
    );

    if ((memberCheck.rowCount ?? 0) === 0) {
      return res.status(403).json({ message: "You are not a member of this league" });
    }

    const leagueResult = await client.query(
      `SELECT season_id, tournament_id FROM fantasydata.leagues WHERE id = $1`,
      [leagueId]
    );

    if ((leagueResult.rowCount ?? 0) === 0) {
      return res.status(404).json({ message: "League not found" });
    }

    const { season_id, tournament_id } = leagueResult.rows[0];

    const playersResult = await client.query(
      `WITH
      all_match_nums AS (
          SELECT DISTINCT fti.match_num
          FROM fantasydata.fantasy_team_instance fti
          JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
          WHERE ft.league_id = $1
      ),

      taken_players AS (
          SELECT DISTINCT fti.match_num, u.player_id
          FROM fantasydata.fantasy_team_instance fti
          JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
          CROSS JOIN LATERAL (VALUES
              (fti.bat1), (fti.bat2), (fti.wicket1),
              (fti.bowl1), (fti.bowl2), (fti.bowl3),
              (fti.all1), (fti.flex1),
              (fti.bench1), (fti.bench2), (fti.bench3),
              (fti.bench4), (fti.bench5), (fti.bench6)
          ) AS u(player_id)
          WHERE ft.league_id = $1
            AND u.player_id IS NOT NULL
      )

      SELECT
          p.id,
          p.player_name,
          p.full_name,
          p.image,
          psi.role,
          ci.name AS country_name,
          ci.image AS country_image,
          psi.rank,
          psi.initial_projection,
          t.name AS team_name,
          t.abbreviation AS team_abbreviation,
          (
              SELECT MAX(amn.match_num)
              FROM all_match_nums amn
              WHERE NOT EXISTS (
                  SELECT 1 FROM taken_players tp
                  WHERE tp.player_id = p.id AND tp.match_num = amn.match_num
              )
          ) AS available_from
      FROM irldata.player_season_info psi
      JOIN irldata.player p ON p.id = psi.player_id
      LEFT JOIN irldata.country_info ci ON ci.id = p.country_id
      LEFT JOIN irldata.team t ON t.id = psi.team_id
      WHERE psi.season_id = $2
        AND psi.tournament_id = $3
      ORDER BY psi.rank ASC
      LIMIT $4 OFFSET $5`,
      [leagueId, season_id, tournament_id, limit, offset]
    );

    return res.status(200).json(playersResult.rows);
  } catch (err) {
    console.error("GET /draft/players/available failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
