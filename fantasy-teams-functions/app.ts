import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   GET FANTASY TEAMS FOR USER
   GET /fantasy-teams/user
   GET /fantasy-teams/user?leagueId=<leagueId>
   ======================================================================================= */
app.get("/fantasy-teams/user", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const leagueId = req.query.leagueId as string | undefined;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = leagueId
      ? `
        SELECT ft.*, p.full_name AS user_name
        FROM fantasydata.fantasy_teams ft
        JOIN authdata.profiles p ON p.user_id = ft.user_id
        WHERE ft.user_id = $1 AND ft.league_id = $2
        ORDER BY ft.created_at ASC;
        `
      : `
        SELECT ft.*, p.full_name AS user_name
        FROM fantasydata.fantasy_teams ft
        JOIN authdata.profiles p ON p.user_id = ft.user_id
        WHERE ft.user_id = $1
        ORDER BY ft.created_at ASC;
        `;

    const values = leagueId ? [userId, leagueId] : [userId];

    const result = await client.query(sql, values);

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /fantasy-teams/user failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET ALL FANTASY TEAMS IN A LEAGUE
   GET /fantasy-teams?leagueId=<leagueId>
   ======================================================================================= */
app.get("/fantasy-teams", async (req: Request, res: Response) => {
  const leagueId = req.query.leagueId as string | undefined;
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!leagueId) {
    return res.status(400).json({ message: "Missing leagueId" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT *
      FROM fantasydata.fantasy_teams
      WHERE league_id = $1
      ORDER BY draft_order ASC;
      `,
      [leagueId]
    );

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /fantasy-teams failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET FANTASY TEAM BY ID (OWNERSHIP REQUIRED)
   GET /fantasy-teams/:fantasyTeamId
   ======================================================================================= */
app.get("/fantasy-teams/:fantasyTeamId", async (req: Request, res: Response) => {
  const { fantasyTeamId } = req.params;

  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!fantasyTeamId) {
    return res.status(400).json({ message: "Missing fantasyTeamId in path" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT *
      FROM fantasydata.fantasy_teams
      WHERE id = $1 AND user_id = $2
      LIMIT 1;
      `,
      [fantasyTeamId, userId]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({
        message: "Fantasy team not found or you do not have access to it"
      });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error(
      "GET /fantasy-teams/:fantasyTeamId failed:",
      err
    );
    return res.status(500).json({
      message: "Internal server error"
    });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
