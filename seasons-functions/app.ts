import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   GET SEASON INFO (TEAMS + MATCHES)
   GET /seasons/:seasonId
   ======================================================================================= */
app.get("/seasons/:seasonId", async (req: Request, res: Response) => {
  const { seasonId } = req.params;

  if (!seasonId) {
    return res.status(400).json({
      message: "null or empty seasonId"
    });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT id, tournament_id, start_year, end_year
      FROM irldata.season
      WHERE id = $1
      LIMIT 1;
      `,
      [seasonId]
    );

    if ((result.rowCount ?? 0) === 0) {
      return res.status(204).json({
        message: "No rows were found that matched this season"
      });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /seasons/:seasonId failed:", err);
    return res.status(500).json({
      message: "Internal server error"
    });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
