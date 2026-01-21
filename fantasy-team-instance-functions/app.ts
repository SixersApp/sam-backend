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
      FROM fantasydata.fantasy_team_instances fti
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

export const lambdaHandler = createHandler(app);

