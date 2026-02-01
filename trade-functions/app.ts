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

    // Logic Check: schema confirms no match_num in fantasydata.trades
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
  } catch (err: any) {
    await client?.query("ROLLBACK");
    console.error("Propose Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET TRADES (LIST)
   GET /trades/list/:teamId
   ======================================================================================= */
app.get("/trades/list/:teamId", async (req: Request, res: Response) => {
  const { teamId } = req.params;
  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    // Logic Check: irldata.player uses 'full_name'
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
  } catch (err: any) {
    console.error("List Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   ACCEPT TRADE (ROSTER SWAP)
   POST /trades/:tradeId/accept
   ======================================================================================= */
app.post("/trades/:tradeId/accept", async (req: Request, res: Response) => {
  const { tradeId } = req.params;
  const { matchNum } = req.body; // matchNum is strictly required for fantasy_team_instance

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();
    await client.query("BEGIN");

    const tradeRes = await client.query(
      `SELECT * FROM fantasydata.trades WHERE id = $1 AND status = 'pending' FOR UPDATE`, 
      [tradeId]
    );
    if (tradeRes.rowCount === 0) return res.status(404).json({ message: "Trade not found" });
    const trade = tradeRes.rows[0];

    // Logic Check: Must use composite key (team_id + match_num)
    const proposerRes = await client.query(
      `SELECT bat1, bat2 FROM fantasydata.fantasy_team_instance WHERE fantasy_team_id = $1 AND match_num = $2`,
      [trade.proposer_fantasy_team_id, matchNum]
    );
    const recipientRes = await client.query(
      `SELECT bat1 FROM fantasydata.fantasy_team_instance WHERE fantasy_team_id = $1 AND match_num = $2`,
      [trade.recipient_fantasy_team_id, matchNum]
    );

    if (proposerRes.rowCount === 0 || recipientRes.rowCount === 0) {
        return res.status(400).json({ message: "Team instance for this match does not exist." });
    }

    const { bat1: pBat1, bat2: pBat2 } = proposerRes.rows[0];
    const { bat1: rBat1 } = recipientRes.rows[0];

    // Perform 2-for-1 Swap Logic targeting the match instance
    await client.query(
      `UPDATE fantasydata.fantasy_team_instance SET bat1 = $1, bat2 = $2 
       WHERE fantasy_team_id = $3 AND match_num = $4`,
      [pBat1, pBat2, trade.recipient_fantasy_team_id, matchNum]
    );

    await client.query(
      `UPDATE fantasydata.fantasy_team_instance SET bat1 = $1, bat2 = NULL 
       WHERE fantasy_team_id = $2 AND match_num = $3`,
      [rBat1, trade.proposer_fantasy_team_id, matchNum]
    );

    await client.query(`UPDATE fantasydata.trades SET status = 'accepted' WHERE id = $1`, [tradeId]);

    await client.query("COMMIT");
    return res.status(200).json({ message: "Trade accepted successfully" });
  } catch (err: any) {
    await client?.query("ROLLBACK");
    console.error("Accept Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   DECLINE TRADE
   PATCH /trades/:tradeId/decline
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
  } catch (err: any) {
    console.error("Decline Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);