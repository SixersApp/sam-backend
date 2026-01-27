import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   PROPOSE A TRADE
   POST /trades/propose
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
   GET TRADES (SENT & RECEIVED)
   GET /trades/list/:teamId
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
          (SELECT json_agg(json_build_object('id', p.id, 'name', p.name))
           FROM fantasydata.trade_offered_players top 
           JOIN irldata.player p ON top.player_id = p.id 
           WHERE top.trade_id = t.id) AS offered_players,
          (SELECT json_agg(json_build_object('id', p.id, 'name', p.name))
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
   ACCEPT TRADE (WITH OWNERSHIP CHECK)
   POST /trades/:tradeId/accept
   ======================================================================================= */
app.post("/trades/:tradeId/accept", async (req: Request, res: Response) => {
  const { tradeId } = req.params;
  let client;

  try {
    const pool = getPool();
    client = await pool.connect();
    await client.query("BEGIN");

    const tradeRes = await client.query(`SELECT * FROM fantasydata.trades WHERE id = $1 AND status = 'pending' FOR UPDATE`, [tradeId]);
    if (tradeRes.rowCount === 0) return res.status(404).json({ message: "Trade not found/active" });
    const trade = tradeRes.rows[0];

    const checkOffered = await client.query(
      `SELECT player_id FROM fantasydata.trade_offered_players WHERE trade_id = $1
       EXCEPT SELECT player_id FROM fantasydata.fantasy_team_instance WHERE fantasy_team_id = $2`,
      [tradeId, trade.proposer_fantasy_team_id]
    );

    const checkRequested = await client.query(
      `SELECT player_id FROM fantasydata.trade_requested_players WHERE trade_id = $1
       EXCEPT SELECT player_id FROM fantasydata.fantasy_team_instance WHERE fantasy_team_id = $2`,
      [tradeId, trade.recipient_fantasy_team_id]
    );

    if (checkOffered.rowCount! > 0 || checkRequested.rowCount! > 0) {
      await client.query(`UPDATE fantasydata.trades SET status = 'invalid' WHERE id = $1`, [tradeId]);
      await client.query("COMMIT");
      return res.status(400).json({ message: "Trade invalid: players have been moved/dropped." });
    }

    await client.query(
      `UPDATE fantasydata.fantasy_team_instance SET fantasy_team_id = $1 
       WHERE player_id IN (SELECT player_id FROM fantasydata.trade_offered_players WHERE trade_id = $2)`,
      [trade.recipient_fantasy_team_id, tradeId]
    );
    await client.query(
      `UPDATE fantasydata.fantasy_team_instance SET fantasy_team_id = $1 
       WHERE player_id IN (SELECT player_id FROM fantasydata.trade_requested_players WHERE trade_id = $2)`,
      [trade.proposer_fantasy_team_id, tradeId]
    );

    await client.query(`UPDATE fantasydata.trades SET status = 'accepted' WHERE id = $1`, [tradeId]);

    await client.query("COMMIT");
    return res.status(200).json({ message: "Trade accepted and rosters updated." });
  } catch (err) {
    await client?.query("ROLLBACK");
    return res.status(500).json({ message: "Unexpected error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   DECLINE TRADE (HANDLES REVOKE & DECLINE SIMPLY)
   PATCH /trades/:tradeId/decline
   ======================================================================================= */
app.patch("/trades/:tradeId/decline", async (req: Request, res: Response) => {
  const { tradeId } = req.params;
  let client;

  try {
    const pool = getPool();
    client = await pool.connect();
    
    // We simply set any pending trade to 'declined'
    const result = await client.query(
      `UPDATE fantasydata.trades SET status = 'declined' WHERE id = $1 AND status = 'pending'`,
      [tradeId]
    );

    if (result.rowCount === 0) {
      return res.status(400).json({ message: "Trade could not be declined." });
    }

    return res.status(200).json({ message: "Trade successfully declined." });
  } catch (err) {
    console.error("PATCH /trades/:tradeId/decline failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);