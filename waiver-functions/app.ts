import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

const SLOTS = ["bat1", "bat2", "bat3", "wicket1", "bowl1", "bowl2", "bowl3", "all1", "flex1", "bench1", "bench2", "bench3", "bench4", "bench5", "bench6"];

function findSlot(slots: Record<string, string | null>, playerId: string): string | null {
  return SLOTS.find(s => slots[s] === playerId) ?? null;
}

function findEmptySlot(slots: Record<string, string | null>): string | null {
  return SLOTS.find(s => !slots[s]) ?? null;
}

// Same three-tier algorithm used by matchup-functions and trade-functions
async function getCurrentMatchNum(
  client: any,
  leagueId: string,
  seasonId: string,
  tournamentId: string
): Promise<number | null> {
  const res = await client.query(`
    WITH match_statuses AS (
      SELECT DISTINCT fm.match_num, mi.status
      FROM fantasydata.fantasy_matchups fm
      JOIN fantasydata.fantasy_team_instance ti1 ON ti1.id = fm.fantasy_team_instance1_id
      JOIN fantasydata.fantasy_team_instance ti2 ON ti2.id = fm.fantasy_team_instance2_id
      CROSS JOIN LATERAL (VALUES
        (ti1.bat1),(ti1.bat2),(ti1.wicket1),(ti1.bowl1),(ti1.bowl2),(ti1.bowl3),(ti1.all1),(ti1.flex1),
        (ti2.bat1),(ti2.bat2),(ti2.wicket1),(ti2.bowl1),(ti2.bowl2),(ti2.bowl3),(ti2.all1),(ti2.flex1)
      ) AS u(player_id)
      JOIN irldata.player_season_info psi
        ON psi.player_id = u.player_id AND psi.season_id = $2 AND psi.tournament_id = $3
      JOIN irldata.match_info mi
        ON mi.tournament_id = $3 AND mi.season_id = $2
        AND (
          (mi.home_team_id = psi.team_id AND mi.home_match_num = fm.match_num)
          OR  (mi.away_team_id = psi.team_id AND mi.away_match_num = fm.match_num)
        )
      WHERE fm.league_id = $1 AND u.player_id IS NOT NULL
    ),
    active_weeks AS (
      SELECT match_num FROM match_statuses GROUP BY match_num
      HAVING
        COUNT(*) FILTER (WHERE status = 'LIVE') > 0
        OR (COUNT(*) FILTER (WHERE status IN ('FINISHED','ABAN.')) > 0
            AND COUNT(*) FILTER (WHERE status IN ('NS','LIVE')) > 0)
    ),
    completed_weeks AS (
      SELECT match_num FROM match_statuses GROUP BY match_num
      HAVING COUNT(*) FILTER (WHERE status IN ('NS','LIVE')) = 0
    )
    SELECT COALESCE(
      (SELECT MIN(match_num) FROM active_weeks),
      (
        SELECT cw_max + 1 FROM (SELECT MAX(match_num) AS cw_max FROM completed_weeks) sub
        WHERE NOT EXISTS (SELECT 1 FROM active_weeks)
          AND EXISTS (
            SELECT 1 FROM fantasydata.fantasy_matchups fm2
            WHERE fm2.league_id = $1 AND fm2.match_num = cw_max + 1
          )
      )
    ) AS current_match_num
  `, [leagueId, seasonId, tournamentId]);
  const val = res.rows[0]?.current_match_num;
  return val != null ? Number(val) : null;
}

// Subquery: player IDs currently rostered in the league for a given match week
const takenSubquery = `
  SELECT DISTINCT u.player_id
  FROM fantasydata.fantasy_team_instance fti
  JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
  CROSS JOIN LATERAL (VALUES
    (fti.bat1),(fti.bat2),(fti.bat3),(fti.wicket1),
    (fti.bowl1),(fti.bowl2),(fti.bowl3),
    (fti.all1),(fti.flex1),
    (fti.bench1),(fti.bench2),(fti.bench3),(fti.bench4),(fti.bench5),(fti.bench6)
  ) AS u(player_id)
  WHERE ft.league_id = $1 AND fti.match_num = $2 AND u.player_id IS NOT NULL
`;

/* =======================================================================================
   LIST AVAILABLE WAIVER PLAYERS
   GET /waivers/:leagueId?page=1&limit=5
   ======================================================================================= */
app.get("/waivers/:leagueId", async (req: Request, res: Response) => {
  const { leagueId } = req.params;
  const page = Math.max(1, parseInt(req.query.page as string) || 1);
  const limit = Math.min(50, Math.max(1, parseInt(req.query.limit as string) || 5));
  const offset = (page - 1) * limit;

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    const leagueRes = await client.query(
      `SELECT season_id, tournament_id FROM fantasydata.leagues WHERE id = $1`,
      [leagueId]
    );
    if (leagueRes.rowCount === 0) return res.status(404).json({ message: "League not found" });

    const { season_id, tournament_id } = leagueRes.rows[0];
    const current_match_num = await getCurrentMatchNum(client, leagueId, season_id, tournament_id);
    if (current_match_num === null) return res.status(400).json({ message: "No active match week" });

    const countRes = await client.query(
      `SELECT COUNT(*) AS total
       FROM irldata.player_season_info psi
       JOIN irldata.player p ON p.id = psi.player_id
       WHERE psi.season_id = $3 AND psi.tournament_id = $4
         AND p.id NOT IN (${takenSubquery})`,
      [leagueId, current_match_num, season_id, tournament_id]
    );
    const total = parseInt(countRes.rows[0].total, 10);
    const totalPages = Math.max(1, Math.ceil(total / limit));

    const playersRes = await client.query(
      `SELECT
         p.id,
         COALESCE(p.full_name, p.name, '') AS name,
         COALESCE(psi.role, '') AS role,
         COALESCE(match_week.status, 'NS') AS match_status
       FROM irldata.player_season_info psi
       JOIN irldata.player p ON p.id = psi.player_id
       LEFT JOIN LATERAL (
         SELECT mi.status
         FROM irldata.match_info mi
         WHERE mi.season_id = psi.season_id
           AND mi.tournament_id = psi.tournament_id
           AND (
             (mi.home_team_id = psi.team_id AND mi.home_match_num = $2)
             OR (mi.away_team_id = psi.team_id AND mi.away_match_num = $2)
           )
         LIMIT 1
       ) match_week ON true
       WHERE psi.season_id = $3 AND psi.tournament_id = $4
         AND p.id NOT IN (${takenSubquery})
       ORDER BY
         CASE WHEN COALESCE(match_week.status, 'NS') NOT IN ('IN_PROGRESS', 'FINISHED') THEN 0 ELSE 1 END,
         p.full_name
       LIMIT $5 OFFSET $6`,
      [leagueId, current_match_num, season_id, tournament_id, limit, offset]
    );

    return res.status(200).json({
      players: playersRes.rows,
      page,
      totalPages,
      total,
    });
  } catch (err: any) {
    console.error("List Waivers Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   ADD PLAYER FROM WAIVERS
   POST /waivers/:leagueId/add
   Body: { teamId, playerId }
   ======================================================================================= */
app.post("/waivers/:leagueId/add", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { leagueId } = req.params;
  const { teamId, playerId } = req.body;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });
  if (!teamId || !playerId) return res.status(400).json({ message: "Missing teamId or playerId" });

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    const ownerCheck = await client.query(
      `SELECT id FROM fantasydata.fantasy_teams WHERE id = $1 AND user_id = $2 AND league_id = $3`,
      [teamId, userId, leagueId]
    );
    if (ownerCheck.rowCount === 0) return res.status(403).json({ message: "You do not own this team" });

    const leagueRes = await client.query(
      `SELECT season_id, tournament_id FROM fantasydata.leagues WHERE id = $1`,
      [leagueId]
    );
    if (leagueRes.rowCount === 0) return res.status(404).json({ message: "League not found" });

    const { season_id, tournament_id } = leagueRes.rows[0];
    const current_match_num = await getCurrentMatchNum(client, leagueId, season_id, tournament_id);
    if (current_match_num === null) return res.status(400).json({ message: "No active match week" });

    // Player must not already be on a team
    const takenCheck = await client.query(
      `SELECT 1 FROM fantasydata.fantasy_team_instance fti
       JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
       CROSS JOIN LATERAL (VALUES
         (fti.bat1),(fti.bat2),(fti.bat3),(fti.wicket1),
         (fti.bowl1),(fti.bowl2),(fti.bowl3),
         (fti.all1),(fti.flex1),
         (fti.bench1),(fti.bench2),(fti.bench3),(fti.bench4),(fti.bench5),(fti.bench6)
       ) AS u(player_id)
       WHERE ft.league_id = $1 AND fti.match_num = $2 AND u.player_id = $3 LIMIT 1`,
      [leagueId, current_match_num, playerId]
    );
    if (takenCheck.rowCount! > 0) return res.status(400).json({ message: "Player is already on a team" });

    // Eligibility: block if match is IN_PROGRESS or FINISHED this week
    const eligRes = await client.query(
      `SELECT 1
       FROM irldata.player_season_info psi
       JOIN irldata.match_info mi
         ON mi.season_id = psi.season_id AND mi.tournament_id = psi.tournament_id
         AND (
           (mi.home_team_id = psi.team_id AND mi.home_match_num = $3)
           OR (mi.away_team_id = psi.team_id AND mi.away_match_num = $3)
         )
       WHERE psi.player_id = $1 AND psi.season_id = $2 AND psi.tournament_id = $4
         AND mi.status IN ('IN_PROGRESS', 'FINISHED')
       LIMIT 1`,
      [playerId, season_id, current_match_num, tournament_id]
    );
    if (eligRes.rowCount! > 0) {
      return res.status(400).json({ message: "Player has already played or is in an active match this week" });
    }

    // Fetch all future instances for the team
    const futureRes = await client.query(
      `SELECT id, ${SLOTS.join(", ")} FROM fantasydata.fantasy_team_instance
       WHERE fantasy_team_id = $1 AND match_num >= $2 ORDER BY match_num`,
      [teamId, current_match_num]
    );
    if (futureRes.rowCount === 0) return res.status(400).json({ message: "No roster found for current match week" });

    // Check space in current instance
    if (!findEmptySlot(futureRes.rows[0])) {
      return res.status(400).json({ message: "Your roster is full — drop a player first" });
    }

    await client.query("BEGIN");
    for (const instance of futureRes.rows) {
      const slot = findEmptySlot(instance);
      if (!slot) continue;
      await client.query(
        `UPDATE fantasydata.fantasy_team_instance SET ${slot} = $2 WHERE id = $1`,
        [instance.id, playerId]
      );
    }
    await client.query("COMMIT");

    return res.status(200).json({ ok: true, message: "Player added to your roster" });
  } catch (err: any) {
    await client?.query("ROLLBACK");
    console.error("Add Waiver Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   DROP PLAYER TO WAIVERS
   POST /waivers/:leagueId/drop
   Body: { teamId, playerId }
   ======================================================================================= */
app.post("/waivers/:leagueId/drop", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { leagueId } = req.params;
  const { teamId, playerId } = req.body;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });
  if (!teamId || !playerId) return res.status(400).json({ message: "Missing teamId or playerId" });

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    const ownerCheck = await client.query(
      `SELECT id FROM fantasydata.fantasy_teams WHERE id = $1 AND user_id = $2 AND league_id = $3`,
      [teamId, userId, leagueId]
    );
    if (ownerCheck.rowCount === 0) return res.status(403).json({ message: "You do not own this team" });

    const leagueRes = await client.query(
      `SELECT season_id, tournament_id FROM fantasydata.leagues WHERE id = $1`,
      [leagueId]
    );
    if (leagueRes.rowCount === 0) return res.status(404).json({ message: "League not found" });

    const { season_id, tournament_id } = leagueRes.rows[0];
    const current_match_num = await getCurrentMatchNum(client, leagueId, season_id, tournament_id);
    if (current_match_num === null) return res.status(400).json({ message: "No active match week" });

    // Player must be on current roster
    const currentRes = await client.query(
      `SELECT id, ${SLOTS.join(", ")}, captain, vice_captain FROM fantasydata.fantasy_team_instance
       WHERE fantasy_team_id = $1 AND match_num = $2`,
      [teamId, current_match_num]
    );
    if (currentRes.rowCount === 0) return res.status(400).json({ message: "No roster found for current match week" });
    if (!findSlot(currentRes.rows[0], playerId)) {
      return res.status(400).json({ message: "Player is not on your current roster" });
    }

    // Eligibility check
    const eligRes = await client.query(
      `SELECT 1
       FROM irldata.player_season_info psi
       JOIN irldata.match_info mi
         ON mi.season_id = psi.season_id AND mi.tournament_id = psi.tournament_id
         AND (
           (mi.home_team_id = psi.team_id AND mi.home_match_num = $3)
           OR (mi.away_team_id = psi.team_id AND mi.away_match_num = $3)
         )
       WHERE psi.player_id = $1 AND psi.season_id = $2 AND psi.tournament_id = $4
         AND mi.status IN ('IN_PROGRESS', 'FINISHED')
       LIMIT 1`,
      [playerId, season_id, current_match_num, tournament_id]
    );
    if (eligRes.rowCount! > 0) {
      return res.status(400).json({ message: "Cannot drop a player who has already played or is in an active match" });
    }

    const futureRes = await client.query(
      `SELECT id, ${SLOTS.join(", ")}, captain, vice_captain FROM fantasydata.fantasy_team_instance
       WHERE fantasy_team_id = $1 AND match_num >= $2 ORDER BY match_num`,
      [teamId, current_match_num]
    );

    await client.query("BEGIN");
    for (const instance of futureRes.rows) {
      const slot = findSlot(instance, playerId);
      if (!slot) continue;
      const updates: string[] = [`${slot} = NULL`];
      if (instance.captain === playerId) updates.push("captain = NULL");
      if (instance.vice_captain === playerId) updates.push("vice_captain = NULL");
      await client.query(
        `UPDATE fantasydata.fantasy_team_instance SET ${updates.join(", ")} WHERE id = $1`,
        [instance.id]
      );
    }
    await client.query("COMMIT");

    return res.status(200).json({ ok: true, message: "Player dropped to waivers" });
  } catch (err: any) {
    await client?.query("ROLLBACK");
    console.error("Drop Waiver Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
