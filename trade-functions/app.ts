import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

const SLOTS = ["bat1", "bat2", "wicket1", "bowl1", "bowl2", "bowl3", "all1", "flex1", "bench1", "bench2", "bench3"];

function findSlot(slots: Record<string, string | null>, playerId: string): string | null {
  return SLOTS.find(s => slots[s] === playerId) ?? null;
}

// Same three-tier algorithm used by matchup-functions to find the current active match week
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

function buildRosterUpdates(
  currentSlots: Record<string, string | null>,
  outgoing: string[],
  incoming: string[]
): Record<string, string | null> {
  const updates: Record<string, string | null> = {};

  const freedSlots: string[] = [];
  for (const pid of outgoing) {
    const slot = findSlot(currentSlots, pid);
    if (!slot) throw new Error(`Player ${pid} not found in roster`);
    updates[slot] = null;
    freedSlots.push(slot);
  }

  // Available slots: freed slots first, then any currently empty slots
  const availableSlots = [...freedSlots];
  for (const slot of SLOTS) {
    if (availableSlots.length >= incoming.length) break;
    if (!availableSlots.includes(slot) && !currentSlots[slot]) {
      availableSlots.push(slot);
    }
  }

  if (availableSlots.length < incoming.length) {
    throw new Error("Not enough roster space for incoming players");
  }

  for (let i = 0; i < incoming.length; i++) {
    updates[availableSlots[i]] = incoming[i];
  }

  return updates;
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
  if (proposerTeamId === recipientTeamId) {
    return res.status(400).json({ message: "Cannot trade with your own team" });
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

    // Verify recipient team is in the same league
    const recipientCheck = await client.query(
      `SELECT id FROM fantasydata.fantasy_teams WHERE id = $1 AND league_id = $2`,
      [recipientTeamId, leagueId]
    );
    if (recipientCheck.rowCount === 0) {
      return res.status(400).json({ message: "Recipient team is not in this league" });
    }

    // Get league season/tournament context
    const leagueRes = await client.query(
      `SELECT season_id, tournament_id FROM fantasydata.leagues WHERE id = $1`,
      [leagueId]
    );
    if (leagueRes.rowCount === 0) return res.status(404).json({ message: "League not found" });

    const { season_id, tournament_id } = leagueRes.rows[0];
    const current_match_num = await getCurrentMatchNum(client, leagueId, season_id, tournament_id);

    if (current_match_num === null) {
      return res.status(400).json({ message: "League has no active match week" });
    }

    // Validate offered players are on proposer's roster and requested players are on recipient's roster
    const proposerRosterRes = await client.query(
      `SELECT ${SLOTS.join(", ")} FROM fantasydata.fantasy_team_instance WHERE fantasy_team_id = $1 AND match_num = $2`,
      [proposerTeamId, current_match_num]
    );
    const recipientRosterRes = await client.query(
      `SELECT ${SLOTS.join(", ")} FROM fantasydata.fantasy_team_instance WHERE fantasy_team_id = $1 AND match_num = $2`,
      [recipientTeamId, current_match_num]
    );
    if (proposerRosterRes.rowCount === 0 || recipientRosterRes.rowCount === 0) {
      return res.status(400).json({ message: "Roster not found for current match week" });
    }
    const proposerRosterSlots: Record<string, string | null> = proposerRosterRes.rows[0];
    const recipientRosterSlots: Record<string, string | null> = recipientRosterRes.rows[0];
    for (const pid of offeredPlayerIds) {
      if (!findSlot(proposerRosterSlots, pid)) {
        return res.status(400).json({ message: `Offered player ${pid} is not on your roster` });
      }
    }
    for (const pid of requestedPlayerIds) {
      if (!findSlot(recipientRosterSlots, pid)) {
        return res.status(400).json({ message: `Requested player ${pid} is not on recipient's roster` });
      }
    }

    // Check roster space: net incoming players must fit into available empty slots
    const proposerEmptySlots = SLOTS.filter(s => !proposerRosterSlots[s]).length;
    const recipientEmptySlots = SLOTS.filter(s => !recipientRosterSlots[s]).length;
    const proposerNetIncoming = requestedPlayerIds.length - offeredPlayerIds.length;
    const recipientNetIncoming = offeredPlayerIds.length - requestedPlayerIds.length;
    if (proposerNetIncoming > proposerEmptySlots) {
      return res.status(400).json({ message: "You do not have enough roster space for the incoming players" });
    }
    if (recipientNetIncoming > recipientEmptySlots) {
      return res.status(400).json({ message: "Recipient does not have enough roster space for the incoming players" });
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
         t.proposer_fantasy_team_id, t.recipient_fantasy_team_id,
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
      `SELECT t.*, ft.user_id AS recipient_user_id, l.season_id, l.tournament_id
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

    const { proposer_fantasy_team_id, recipient_fantasy_team_id, league_id, season_id, tournament_id } = trade;
    const current_match_num = await getCurrentMatchNum(client, league_id, season_id, tournament_id);

    if (current_match_num === null) {
      return res.status(400).json({ message: "League has no active match week" });
    }

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

    // Validate against current instance first
    const proposerCurrentRes = await client.query(
      `SELECT id, ${SLOTS.join(", ")}, captain, vice_captain FROM fantasydata.fantasy_team_instance
       WHERE fantasy_team_id = $1 AND match_num = $2`,
      [proposer_fantasy_team_id, current_match_num]
    );
    const recipientCurrentRes = await client.query(
      `SELECT id, ${SLOTS.join(", ")}, captain, vice_captain FROM fantasydata.fantasy_team_instance
       WHERE fantasy_team_id = $1 AND match_num = $2`,
      [recipient_fantasy_team_id, current_match_num]
    );
    if (proposerCurrentRes.rowCount === 0 || recipientCurrentRes.rowCount === 0) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Team roster not found for current match week" });
    }

    try {
      buildRosterUpdates(proposerCurrentRes.rows[0], offeredPlayerIds, requestedPlayerIds);
      buildRosterUpdates(recipientCurrentRes.rows[0], requestedPlayerIds, offeredPlayerIds);
    } catch (err: any) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: err.message });
    }

    // Fetch all future instances (current week and beyond) and apply the swap to each
    const proposerFutureRes = await client.query(
      `SELECT id, ${SLOTS.join(", ")}, captain, vice_captain FROM fantasydata.fantasy_team_instance
       WHERE fantasy_team_id = $1 AND match_num >= $2 ORDER BY match_num`,
      [proposer_fantasy_team_id, current_match_num]
    );
    const recipientFutureRes = await client.query(
      `SELECT id, ${SLOTS.join(", ")}, captain, vice_captain FROM fantasydata.fantasy_team_instance
       WHERE fantasy_team_id = $1 AND match_num >= $2 ORDER BY match_num`,
      [recipient_fantasy_team_id, current_match_num]
    );

    const applyToInstances = async (instances: any[], outgoing: string[], incoming: string[]) => {
      for (const instance of instances) {
        let updates: Record<string, string | null>;
        try {
          updates = buildRosterUpdates(instance, outgoing, incoming);
        } catch {
          continue; // player already gone from this future instance (e.g. a prior trade)
        }
        for (const pid of outgoing) {
          if (instance.captain === pid) updates.captain = null;
          if (instance.vice_captain === pid) updates.vice_captain = null;
        }
        if (Object.keys(updates).length === 0) continue;
        const setClauses = Object.keys(updates).map((col, i) => `${col} = $${i + 2}`);
        await client.query(
          `UPDATE fantasydata.fantasy_team_instance SET ${setClauses.join(", ")} WHERE id = $1`,
          [instance.id, ...Object.values(updates)]
        );
      }
    };

    await applyToInstances(proposerFutureRes.rows, offeredPlayerIds, requestedPlayerIds);
    await applyToInstances(recipientFutureRes.rows, requestedPlayerIds, offeredPlayerIds);

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
