import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   PROPOSE A TRADE (Updated with match_num)
   ======================================================================================= */
app.post("/trades/propose", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  // Added matchNum to the request body
  const { leagueId, proposerTeamId, recipientTeamId, offeredPlayerIds, requestedPlayerIds, matchNum } = req.body;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();
    await client.query("BEGIN");

    // 1. Insert Trade Header (Consider adding match_num to your trades table if not there)
    const tradeResult = await client.query(
      `INSERT INTO fantasydata.trades (league_id, proposer_fantasy_team_id, recipient_fantasy_team_id, status)
       VALUES ($1, $2, $3, 'pending') RETURNING id`,
      [leagueId, proposerTeamId, recipientTeamId]
    );
    const tradeId = tradeResult.rows[0].id;

    // 2. Insert Offered/Requested Players
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
   ACCEPT TRADE (Updated to target specific match_num)
   ======================================================================================= */
app.post("/trades/:tradeId/accept", async (req: Request, res: Response) => {
  const { tradeId } = req.params;
  const { matchNum } = req.body; // You need to know which match this trade applies to

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

    // 2. Extract current player UUIDs using the specific match_num
    const proposerRes = await client.query(
      `SELECT bat1, bat2 FROM fantasydata.fantasy_team_instance 
       WHERE fantasy_team_id = $1 AND match_num = $2`,
      [trade.proposer_fantasy_team_id, matchNum]
    );
    const recipientRes = await client.query(
      `SELECT bat1 FROM fantasydata.fantasy_team_instance 
       WHERE fantasy_team_id = $1 AND match_num = $2`,
      [trade.recipient_fantasy_team_id, matchNum]
    );

    if (proposerRes.rowCount === 0 || recipientRes.rowCount === 0) {
        return res.status(400).json({ message: "Team instance for this match does not exist." });
    }

    const pBat1 = proposerRes.rows[0].bat1;
    const pBat2 = proposerRes.rows[0].bat2;
    const rBat1 = recipientRes.rows[0].bat1;

    // 3. Swap logic targeting the specific match_num instance
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
    return res.status(200).json({ message: "Trade accepted: instance updated." });
  } catch (err) {
    await client?.query("ROLLBACK");
    console.error("Accept failed:", err);
    return res.status(500).json({ message: "Internal error" });
  } finally {
    client?.release();
  }
});