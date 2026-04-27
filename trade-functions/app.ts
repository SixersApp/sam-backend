import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

const app = createApp();
const sqsClient = new SQSClient({});
const TRADE_EXECUTION_QUEUE_URL = process.env.TRADE_EXECUTION_QUEUE_URL!;
const TRADE_DELAY_SECONDS = parseInt(process.env.TRADE_DELAY_SECONDS ?? "60", 10);

const SLOTS = ["bat1", "bat2", "bat3", "wicket1", "bowl1", "bowl2", "bowl3", "all1", "flex1", "bench1", "bench2", "bench3", "bench4", "bench5", "bench6"];

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
         t.id, t.status, t.created_at, t.expires_at, t.responded_at,
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
   GET ALL TRADES FOR A LEAGUE (transaction history)
   GET /trades/league/:leagueId
   ======================================================================================= */
app.get("/trades/league/:leagueId", async (req: Request, res: Response) => {
  const { leagueId } = req.params;
  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `SELECT
         t.id, t.status, t.created_at, t.expires_at, t.responded_at,
         p_team.team_name AS from_team,
         r_team.team_name AS to_team,
         p_profile.full_name AS proposer_name,
         r_profile.full_name AS recipient_name,
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
       LEFT JOIN authdata.profiles p_profile ON p_profile.user_id = p_team.user_id
       LEFT JOIN authdata.profiles r_profile ON r_profile.user_id = r_team.user_id
       WHERE t.league_id = $1
       ORDER BY t.created_at DESC`,
      [leagueId]
    );
    return res.status(200).json(result.rows);
  } catch (err: any) {
    console.error("League Trades Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   ACCEPT TRADE
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
      `SELECT t.*, ft.user_id AS recipient_user_id
       FROM fantasydata.trades t
       JOIN fantasydata.fantasy_teams ft ON ft.id = t.recipient_fantasy_team_id
       WHERE t.id = $1 AND t.status = 'pending'
       FOR UPDATE`,
      [tradeId]
    );
    if (tradeRes.rowCount === 0) return res.status(404).json({ message: "Trade not found or already resolved" });

    const trade = tradeRes.rows[0];
    if (trade.recipient_user_id !== userId) {
      return res.status(403).json({ message: "Only the trade recipient can accept" });
    }

    await client.query(
      `UPDATE fantasydata.trades
       SET status = 'accepted',
           responded_at = NOW(),
           expires_at = NOW() + ($1 || ' seconds')::interval
       WHERE id = $2`,
      [TRADE_DELAY_SECONDS, tradeId]
    );

    await sqsClient.send(new SendMessageCommand({
      QueueUrl: TRADE_EXECUTION_QUEUE_URL,
      MessageBody: JSON.stringify({ tradeId }),
      DelaySeconds: TRADE_DELAY_SECONDS,
    }));

    await client.query("COMMIT");
    return res.status(200).json({ message: "Trade accepted, pending execution" });
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
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { tradeId } = req.params;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    const tradeRes = await client.query(
      `SELECT t.id, ft.user_id AS recipient_user_id
       FROM fantasydata.trades t
       JOIN fantasydata.fantasy_teams ft ON ft.id = t.recipient_fantasy_team_id
       WHERE t.id = $1 AND t.status = 'pending'`,
      [tradeId]
    );
    if (tradeRes.rowCount === 0) return res.status(404).json({ message: "Trade not found or already resolved" });
    if (tradeRes.rows[0].recipient_user_id !== userId) {
      return res.status(403).json({ message: "Only the trade recipient can decline" });
    }

    await client.query(
      `UPDATE fantasydata.trades SET status = 'declined', responded_at = NOW() WHERE id = $1`,
      [tradeId]
    );
    return res.status(200).json({ message: "Trade declined" });
  } catch (err: any) {
    console.error("Decline Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   CANCEL TRADE (proposer only, while pending)
   PATCH /trades/:tradeId/cancel
   ======================================================================================= */
app.patch("/trades/:tradeId/cancel", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { tradeId } = req.params;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    const tradeRes = await client.query(
      `SELECT t.id, ft.user_id AS proposer_user_id
       FROM fantasydata.trades t
       JOIN fantasydata.fantasy_teams ft ON ft.id = t.proposer_fantasy_team_id
       WHERE t.id = $1 AND t.status = 'pending'`,
      [tradeId]
    );
    if (tradeRes.rowCount === 0) return res.status(404).json({ message: "Trade not found or already resolved" });
    if (tradeRes.rows[0].proposer_user_id !== userId) {
      return res.status(403).json({ message: "Only the trade proposer can cancel" });
    }

    await client.query(
      `UPDATE fantasydata.trades SET status = 'cancelled' WHERE id = $1`,
      [tradeId]
    );
    return res.status(200).json({ message: "Trade cancelled" });
  } catch (err: any) {
    console.error("Cancel Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   VETO TRADE (commissioner only, while accepted and before expires_at)
   POST /trades/:tradeId/veto
   ======================================================================================= */
app.post("/trades/:tradeId/veto", async (req: Request, res: Response) => {
  const userId = req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { tradeId } = req.params;

  if (!userId) return res.status(401).json({ message: "Unauthorized" });

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    const tradeRes = await client.query(
      `SELECT t.*, l.creator_id
       FROM fantasydata.trades t
       JOIN fantasydata.leagues l ON l.id = t.league_id
       WHERE t.id = $1`,
      [tradeId]
    );
    if (tradeRes.rowCount === 0) return res.status(404).json({ message: "Trade not found" });

    const trade = tradeRes.rows[0];

    if (trade.creator_id !== userId) {
      return res.status(403).json({ message: "Only the league commissioner can veto trades" });
    }
    if (trade.status !== "accepted") {
      return res.status(400).json({ message: "Only accepted trades can be vetoed" });
    }
    if (new Date() >= new Date(trade.expires_at)) {
      return res.status(400).json({ message: "Veto window has expired" });
    }

    await client.query(
      `UPDATE fantasydata.trades SET status = 'vetoed' WHERE id = $1`,
      [tradeId]
    );
    return res.status(200).json({ message: "Trade vetoed" });
  } catch (err: any) {
    console.error("Veto Error:", err.message);
    return res.status(500).json({ error: err.message });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);