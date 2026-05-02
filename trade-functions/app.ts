import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

const SLOTS = ["bat1", "bat2", "bat3", "wicket1", "bowl1", "bowl2", "bowl3", "all1", "flex1", "bench1", "bench2", "bench3", "bench4", "bench5", "bench6"];
const SLOT_PRIORITY = ["bat1", "bat2", "bat3", "wicket1", "bowl1", "bowl2", "bowl3", "all1", "flex1", "bench1", "bench2", "bench3", "bench4", "bench5", "bench6"];

function findSlot(slots: Record<string, string | null>, playerId: string): string | null {
  return SLOT_PRIORITY.find(s => slots[s] === playerId) ?? null;
}

/* =======================================================================================
   PROPOSE A TRADE
   POST /trades/propose
   ======================================================================================= */
app.post("/trades/propose", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { leagueId, proposerTeamId, recipientTeamId, offeredPlayerIds, requestedPlayerIds } = req.body;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });
  if (!leagueId || !proposerTeamId || !recipientTeamId) {
    return res.status(400).json({ message: "Missing required fields" });
  }
  if (!Array.isArray(offeredPlayerIds) || offeredPlayerIds.length === 0) {
    return res.status(400).json({ message: "Must offer at least one player" });
  }
  if (!Array.isArray(requestedPlayerIds) || requestedPlayerIds.length === 0) {
    return res.status(400).json({ message: "Must request at least one player" });
  }

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    // Verify proposer team belongs to this user
    const ownerCheck = await client.query(
      `SELECT id FROM fantasydata.fantasy_teams WHERE id = $1 AND user_id = $2 AND league_id = $3`,
      [proposerTeamId, userId, leagueId]
    );
    if (ownerCheck.rowCount === 0) {
      return res.status(403).json({ message: "You do not own this team" });
    }

    // Get league season/tournament context and current match_num
    const leagueRes = await client.query(
      `SELECT l.season_id, l.tournament_id,
         (SELECT MAX(fti.match_num)
          FROM fantasydata.fantasy_team_instance fti
          JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
          WHERE ft.league_id = l.id) AS current_match_num
       FROM fantasydata.leagues l WHERE l.id = $1`,
      [leagueId]
    );
    if (leagueRes.rowCount === 0) return res.status(404).json({ message: "League not found" });

    const { season_id, tournament_id, current_match_num } = leagueRes.rows[0];

    if (current_match_num === null) {
      return res.status(400).json({ message: "League has no active match week" });
    }

    // Check player eligibility — block if their real match this week is IN_PROGRESS or FINISHED
    const allPlayerIds = [...offeredPlayerIds, ...requestedPlayerIds];
    const eligibilityRes = await client.query(
      `SELECT p.id, p.full_name, mi.status AS match_status
       FROM irldata.player p
       JOIN irldata.player_season_info psi
         ON psi.player_id = p.id
         AND psi.season_id = $2
         AND psi.tournament_id = $3
       JOIN irldata.match_info mi
         ON mi.season_id = $2
         AND (
           (mi.home_team_id = psi.team_id AND mi.home_match_num = $4)
           OR
           (mi.away_team_id = psi.team_id AND mi.away_match_num = $4)
         )
       WHERE p.id = ANY($1)
         AND mi.status IN ('IN_PROGRESS', 'FINISHED')`,
      [allPlayerIds, season_id, tournament_id, current_match_num]
    );

    if (eligibilityRes.rowCount! > 0) {
      const blocked = eligibilityRes.rows.map((r: any) => r.full_name);
      return res.status(400).json({
        message: "Some players are ineligible for trading",
        ineligiblePlayers: blocked,
      });
    }

    await client.query("BEGIN");

    const tradeResult = await client.query(
      `INSERT INTO fantasydata.trades (league_id, proposer_fantasy_team_id, recipient_fantasy_team_id, status)
       VALUES ($1, $2, $3, 'pending') RETURNING id`,
      [leagueId, proposerTeamId, recipientTeamId]
    );
    const tradeId = tradeResult.rows[0].id;

    for (const pid of offeredPlayerIds) {
      await client.query(
        `INSERT INTO fantasydata.trade_offered_players (trade_id, player_id) VALUES ($1, $2)`,
        [tradeId, pid]
      );
    }
    for (const pid of requestedPlayerIds) {
      await client.query(
        `INSERT INTO fantasydata.trade_requested_players (trade_id, player_id) VALUES ($1, $2)`,
        [tradeId, pid]
      );
    }

    await client.query("COMMIT");
    return res.status(201).json({ message: "Trade proposed successfully", tradeId });
  } catch (err: any) {
    await client?.query("ROLLBACK");
    console.error("Propose Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET TRADES FOR A TEAM
   GET /trades/list/:teamId
   ======================================================================================= */
app.get("/trades/list/:teamId", async (req: Request, res: Response) => {
  const { teamId } = req.params;
  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `SELECT
         t.id, t.status, t.created_at, t.responded_at,
         p_team.team_name AS from_team,
         r_team.team_name AS to_team,
         (SELECT json_agg(json_build_object('id', p.id, 'name', p.full_name))
          FROM fantasydata.trade_offered_players top
          JOIN irldata.player p ON top.player_id = p.id
          WHERE top.trade_id = t.id) AS offered_players,
         (SELECT json_agg(json_build_object('id', p.id, 'name', p.full_name))
          FROM fantasydata.trade_requested_players trp
          JOIN irldata.player p ON trp.player_id = p.id
          WHERE trp.trade_id = t.id) AS requested_players
       FROM fantasydata.trades t
       JOIN fantasydata.fantasy_teams p_team ON t.proposer_fantasy_team_id = p_team.id
       JOIN fantasydata.fantasy_teams r_team ON t.recipient_fantasy_team_id = r_team.id
       WHERE t.proposer_fantasy_team_id = $1 OR t.recipient_fantasy_team_id = $1
       ORDER BY t.created_at DESC`,
      [teamId]
    );
    return res.status(200).json(result.rows);
  } catch (err: any) {
    console.error("List Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   ACCEPT TRADE — executes roster swap synchronously
   POST /trades/:tradeId/accept
   ======================================================================================= */
app.post("/trades/:tradeId/accept", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { tradeId } = req.params;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();
    await client.query("BEGIN");

    const tradeRes = await client.query(
      `SELECT t.*,
         ft.user_id AS recipient_user_id,
         l.season_id, l.tournament_id,
         (SELECT MAX(fti.match_num)
          FROM fantasydata.fantasy_team_instance fti
          JOIN fantasydata.fantasy_teams ftt ON ftt.id = fti.fantasy_team_id
          WHERE ftt.league_id = t.league_id) AS current_match_num
       FROM fantasydata.trades t
       JOIN fantasydata.fantasy_teams ft ON ft.id = t.recipient_fantasy_team_id
       JOIN fantasydata.leagues l ON l.id = t.league_id
       WHERE t.id = $1 AND t.status = 'pending'
       FOR UPDATE`,
      [tradeId]
    );
    if (tradeRes.rowCount === 0) return res.status(404).json({ message: "Trade not found or already resolved" });

    const trade = tradeRes.rows[0];
    if (trade.recipient_user_id !== userId) {
      return res.status(403).json({ message: "Only the trade recipient can accept" });
    }
    if (trade.current_match_num === null) {
      return res.status(400).json({ message: "League has no active match week" });
    }

    const { proposer_fantasy_team_id, recipient_fantasy_team_id, current_match_num } = trade;

    const offeredRes = await client.query(
      `SELECT player_id FROM fantasydata.trade_offered_players WHERE trade_id = $1`,
      [tradeId]
    );
    const requestedRes = await client.query(
      `SELECT player_id FROM fantasydata.trade_requested_players WHERE trade_id = $1`,
      [tradeId]
    );
    const offeredPlayerIds: string[] = offeredRes.rows.map((r: any) => r.player_id);
    const requestedPlayerIds: string[] = requestedRes.rows.map((r: any) => r.player_id);

    const proposerInstanceRes = await client.query(
      `SELECT ${SLOTS.join(", ")} FROM fantasydata.fantasy_team_instance
       WHERE fantasy_team_id = $1 AND match_num = $2`,
      [proposer_fantasy_team_id, current_match_num]
    );
    const recipientInstanceRes = await client.query(
      `SELECT ${SLOTS.join(", ")} FROM fantasydata.fantasy_team_instance
       WHERE fantasy_team_id = $1 AND match_num = $2`,
      [recipient_fantasy_team_id, current_match_num]
    );

    if (proposerInstanceRes.rowCount === 0 || recipientInstanceRes.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Team roster not found for current match week" });
    }

    const proposerSlots: Record<string, string | null> = proposerInstanceRes.rows[0];
    const recipientSlots: Record<string, string | null> = recipientInstanceRes.rows[0];

    const proposerUpdates: Record<string, string | null> = {};
    const recipientUpdates: Record<string, string | null> = {};

    for (let i = 0; i < offeredPlayerIds.length; i++) {
      const slot = findSlot(proposerSlots, offeredPlayerIds[i]);
      if (slot) proposerUpdates[slot] = requestedPlayerIds[i] ?? null;
    }
    for (let i = 0; i < requestedPlayerIds.length; i++) {
      const slot = findSlot(recipientSlots, requestedPlayerIds[i]);
      if (slot) recipientUpdates[slot] = offeredPlayerIds[i] ?? null;
    }

    if (Object.keys(proposerUpdates).length > 0) {
      const setClauses = Object.keys(proposerUpdates).map((col, i) => `${col} = $${i + 3}`);
      await client.query(
        `UPDATE fantasydata.fantasy_team_instance SET ${setClauses.join(", ")} WHERE fantasy_team_id = $1 AND match_num = $2`,
        [proposer_fantasy_team_id, current_match_num, ...Object.values(proposerUpdates)]
      );
    }
    if (Object.keys(recipientUpdates).length > 0) {
      const setClauses = Object.keys(recipientUpdates).map((col, i) => `${col} = $${i + 3}`);
      await client.query(
        `UPDATE fantasydata.fantasy_team_instance SET ${setClauses.join(", ")} WHERE fantasy_team_id = $1 AND match_num = $2`,
        [recipient_fantasy_team_id, current_match_num, ...Object.values(recipientUpdates)]
      );
    }

    await client.query(
      `UPDATE fantasydata.trades SET status = 'completed', responded_at = NOW() WHERE id = $1`,
      [tradeId]
    );

    await client.query("COMMIT");
    return res.status(200).json({ message: "Trade accepted and executed" });
  } catch (err: any) {
    await client?.query("ROLLBACK");
    console.error("Accept Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   DECLINE / CANCEL TRADE
   - Recipient declining a pending trade → status: declined
   - Proposer cancelling their own pending trade → status: cancelled
   PATCH /trades/:tradeId/decline
   ======================================================================================= */
app.patch("/trades/:tradeId/decline", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { tradeId } = req.params;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    const tradeRes = await client.query(
      `SELECT t.id, t.status,
         proposer_team.user_id AS proposer_user_id,
         recipient_team.user_id AS recipient_user_id
       FROM fantasydata.trades t
       JOIN fantasydata.fantasy_teams proposer_team ON proposer_team.id = t.proposer_fantasy_team_id
       JOIN fantasydata.fantasy_teams recipient_team ON recipient_team.id = t.recipient_fantasy_team_id
       WHERE t.id = $1 AND t.status = 'pending'`,
      [tradeId]
    );
    if (tradeRes.rowCount === 0) return res.status(404).json({ message: "Trade not found or already resolved" });

    const trade = tradeRes.rows[0];

    let newStatus: string;
    let message: string;
    if (trade.recipient_user_id === userId) {
      newStatus = "declined";
      message = "Trade declined";
    } else if (trade.proposer_user_id === userId) {
      newStatus = "cancelled";
      message = "Trade cancelled";
    } else {
      return res.status(403).json({ message: "You are not a party to this trade" });
    }

    await client.query(
      `UPDATE fantasydata.trades SET status = $1, responded_at = NOW() WHERE id = $2`,
      [newStatus, tradeId]
    );
    return res.status(200).json({ message });
  } catch (err: any) {
    console.error("Decline Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
