import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   PROPOSE A TRADE
   ======================================================================================= */
app.post("/trades/propose", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { leagueId, proposerTeamId, recipientTeamId, offeredPlayerIds, requestedPlayerIds } = req.body;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();
    await client.query("BEGIN");

    const tradeResult = await client.query(
      `INSERT INTO fantasydata.trades (league_id, proposer_fantasy_team_id, recipient_fantasy_team_id, status)
       VALUES ($1, $2, $3, 'pending') RETURNING id`,
      [leagueId, proposerTeamId, recipientTeamId]
    );
    const tradeId = tradeResult.rows[0].id;

    for (const pid of offeredPlayerIds) {
      await client.query(`INSERT INTO fantasydata.trade_offered_players (trade_id, player_id) VALUES ($1, $2)`, [tradeId, pid]);
    }

    for (const pid of requestedPlayerIds) {
      await client.query(`INSERT INTO fantasydata.trade_requested_players (trade_id, player_id) VALUES ($1, $2)`, [tradeId, pid]);
    }

    await client.query("COMMIT");
    return res.status(201).json({ message: "Trade proposed successfully", tradeId });
  } catch (err) {
    await client?.query("ROLLBACK");
    console.error("POST /trades/propose failed:", err);
    return res.status(500).json({ message: "Failed to propose trade" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET TRADES
   ======================================================================================= */
app.get("/trades/list/:teamId", async (req: Request, res: Response) => {
  const { teamId } = req.params;
  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      SELECT 
          t.id, t.status, t.created_at,
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
      ORDER BY t.created_at DESC;
    `;

    const result = await client.query(sql, [teamId]);
    return res.status(200).json(result.rows);
  } catch (err) {
    console.error("GET /trades/list failed:", err);
    return res.status(500).json({ message: "Unexpected error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   ACCEPT TRADE (TEST MODE: SWAP BAT1/BAT2)
   ======================================================================================= */
app.post("/trades/:tradeId/accept", async (req: Request, res: Response) => {
  const { tradeId } = req.params;
  let client;

  try {
    const pool = getPool();
    client = await pool.connect();
    await client.query("BEGIN");

    // 1. Fetch trade info
    const tradeRes = await client.query(
      `SELECT * FROM fantasydata.trades WHERE id = $1 AND status = 'pending' FOR UPDATE`, 
      [tradeId]
    );
    if (tradeRes.rowCount === 0) return res.status(404).json({ message: "Trade not found" });
    const trade = tradeRes.rows[0];

    // 2. Extract current player UUIDs from the roster slots
    const proposerRes = await client.query(
      `SELECT bat1, bat2 FROM fantasydata.fantasy_team_instance WHERE fantasy_team_id = $1`,
      [trade.proposer_fantasy_team_id]
    );
    const recipientRes = await client.query(
      `SELECT bat1 FROM fantasydata.fantasy_team_instance WHERE fantasy_team_id = $1`,
      [trade.recipient_fantasy_team_id]
    );

    const pBat1 = proposerRes.rows[0].bat1;
    const pBat2 = proposerRes.rows[0].bat2;
    const rBat1 = recipientRes.rows[0].bat1;

    // 3. Swap: Recipient gets Proposer's bat1 and bat2
    await client.query(
      `UPDATE fantasydata.fantasy_team_instance SET bat1 = $1, bat2 = $2 WHERE fantasy_team_id = $3`,
      [pBat1, pBat2, trade.recipient_fantasy_team_id]
    );

    // 4. Swap: Proposer gets Recipient's bat1 (and we null their bat2 for the 2-for-1 test)
    await client.query(
      `UPDATE fantasydata.fantasy_team_instance SET bat1 = $1, bat2 = NULL WHERE fantasy_team_id = $2`,
      [rBat1, trade.proposer_fantasy_team_id]
    );

    // 5. Finalize
    await client.query(`UPDATE fantasydata.trades SET status = 'accepted' WHERE id = $1`, [tradeId]);

    await client.query("COMMIT");
    return res.status(200).json({ message: "Trade accepted: bat slots updated." });
  } catch (err) {
    await client?.query("ROLLBACK");
    console.error("Accept failed:", err);
    return res.status(500).json({ message: "Internal error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   DECLINE TRADE
   ======================================================================================= */
app.patch("/trades/:tradeId/decline", async (req: Request, res: Response) => {
  const { tradeId } = req.params;
  let client;
  try {
    const pool = getPool();
    client = await pool.connect();
    const result = await client.query(
      `UPDATE fantasydata.trades SET status = 'declined' WHERE id = $1 AND status = 'pending'`,
      [tradeId]
    );
    if (result.rowCount === 0) return res.status(400).json({ message: "Cannot decline trade" });
    return res.status(200).json({ message: "Trade declined" });
  } catch (err) {
    return res.status(500).json({ message: "Unexpected error" });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);