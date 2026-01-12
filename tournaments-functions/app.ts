import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   GET ALL TOURNAMENTS
   GET /tournaments
   ======================================================================================= */
app.get("/tournaments", async (req: Request, res: Response) => {
  const tokenUserId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!tokenUserId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT
        t.id,
        t.name,
        t.abbreviation,
        (
          SELECT json_agg(s.* ORDER BY s.start_year DESC)
          FROM irldata.season s
          WHERE s.tournament_id = t.id
        ) AS seasons
      FROM irldata.tournament_info t
      ORDER BY t.name;
      `
    );

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /tournaments failed:", err);
    return res.status(500).json({
      message: "Internal server error"
    });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET TOURNAMENT INFO (PATH PARAMETER BASED)
   ======================================================================================= */
app.get("/tournaments/:tournamentId", async (req: Request, res: Response) => {

  const { tournamentId } = req.params;

  const tokenUserId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!tokenUserId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!tournamentId) {
    return res.status(400).json({
      message: "Missing tournamentId in path"
    });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT
        t.*,
        (
          SELECT json_agg(s.*)
          FROM irldata.season s
          WHERE s.tournament_id = t.id
        ) AS seasons,
        (
          SELECT json_agg(v.*)
          FROM irldata.venue_info v
          WHERE v.tournament_id = t.id
        ) AS venues
      FROM irldata.tournament_info t
      WHERE t.id = $1;
      `,
      [tournamentId]
    );

    if ((result.rowCount ?? 0) === 0) {
      return res.status(404).json({
        message: "No rows were found that matched this tournament id"
      });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /tournaments/:tournamentId failed:", err);
    return res.status(500).json({
      message: "Internal server error"
    });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET SEASONS FOR TOURNAMENT
   GET /tournaments/:tournamentId/seasons
   ======================================================================================= */
app.get("/tournaments/:tournamentId/seasons", async (req: Request, res: Response) => {

    const { tournamentId} = req.params;

    const tokenUserId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

    if (!tokenUserId) {
      return res.status(401).json({ message: "Unauthorized" });
    }

    if (!tournamentId) {
      return res.status(400).json({
        message: "Missing path parameters"
      });
    }

    let client;

    try {
      const pool = getPool();
      client = await pool.connect();

      const result = await client.query(
        `
        SELECT id, start_year, end_year
        FROM irldata.season
        WHERE tournament_id = $1
        ORDER BY start_year;
        `,
        [tournamentId]
      );

      return res.status(200).json({
        tournamentId,
        seasons: result.rows.map((row: { id: string; start_year: number; end_year: number }) => ({
          id: row.id,
          start_year: row.start_year,
          end_year: row.end_year
        }))
      });

    } catch (err) {
      console.error(
        "GET /tournaments/:tournamentId/seasons failed:",
        err
      );
      return res.status(500).json({
        message: "Internal server error"
      });
    } finally {
      client?.release();
    }
  }
);

export const lambdaHandler = createHandler(app);
