import { Pool } from "pg";
import type { SQSEvent } from "aws-lambda";

const SLOTS = ["bat1", "bat2", "bat3", "wicket1", "bowl1", "bowl2", "bowl3", "all1", "flex1", "bench1", "bench2", "bench3", "bench4", "bench5", "bench6"];

let pool: Pool | null = null;
function getPool(): Pool {
  if (!pool) {
    pool = new Pool({
      host: process.env.DB_HOST,
      port: parseInt(process.env.DB_PORT ?? "5432", 10),
      database: process.env.DB_NAME,
      user: process.env.DB_USER,
      password: process.env.DB_PASSWORD,
      ssl: { rejectUnauthorized: false },
      max: 1,
    });
  }
  return pool;
}

export const lambdaHandler = async (event: SQSEvent): Promise<void> => {
  for (const record of event.Records) {
    const { tradeId } = JSON.parse(record.body);
    console.log("TradeExecutor processing tradeId:", tradeId);

    const client = await getPool().connect();
    try {
      await client.query("BEGIN");

      // Fetch trade — bail if it was vetoed or cancelled during the delay window
      const tradeRes = await client.query(
        `SELECT t.*,
           l.season_id, l.tournament_id,
           (SELECT MAX(fti.match_num)
            FROM fantasydata.fantasy_team_instance fti
            JOIN fantasydata.fantasy_teams ft ON ft.id = fti.fantasy_team_id
            WHERE ft.league_id = t.league_id) AS current_match_num
         FROM fantasydata.trades t
         JOIN fantasydata.leagues l ON l.id = t.league_id
         WHERE t.id = $1
         FOR UPDATE`,
        [tradeId]
      );

      if (tradeRes.rowCount === 0) {
        console.log("Trade not found:", tradeId);
        await client.query("ROLLBACK");
        continue;
      }

      const trade = tradeRes.rows[0];

      if (trade.status !== "accepted") {
        console.log(`Trade ${tradeId} is ${trade.status}, skipping execution`);
        await client.query("ROLLBACK");
        continue;
      }

      const { proposer_fantasy_team_id, recipient_fantasy_team_id, current_match_num } = trade;

      if (current_match_num === null) {
        console.error("No current match_num for league, cannot execute trade:", tradeId);
        await client.query("ROLLBACK");
        continue;
      }

      // Fetch offered and requested player IDs
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

      // Fetch both team instances for the current match week
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
        console.error("Team instance not found for match_num:", current_match_num);
        await client.query("ROLLBACK");
        continue;
      }

      const proposerSlots: Record<string, string | null> = proposerInstanceRes.rows[0];
      const recipientSlots: Record<string, string | null> = recipientInstanceRes.rows[0];

      // Find which slot each player currently occupies, active slots take priority over bench
      const activeSlots = ["bat1", "bat2", "bat3", "wicket1", "bowl1", "bowl2", "bowl3", "all1", "flex1"];
      const benchSlots = ["bench1", "bench2", "bench3", "bench4", "bench5", "bench6"];
      const slotPriority = [...activeSlots, ...benchSlots];

      function findSlot(slots: Record<string, string | null>, playerId: string): string | null {
        return slotPriority.find(s => slots[s] === playerId) ?? null;
      }

      // Find slots for offered players (on proposer) and requested players (on recipient)
      const offeredSlots: string[] = [];
      for (const playerId of offeredPlayerIds) {
        const slot = findSlot(proposerSlots, playerId);
        if (!slot) {
          console.error(`Offered player ${playerId} not found in proposer's roster`);
          continue;
        }
        offeredSlots.push(slot);
      }

      const requestedSlots: string[] = [];
      for (const playerId of requestedPlayerIds) {
        const slot = findSlot(recipientSlots, playerId);
        if (!slot) {
          console.error(`Requested player ${playerId} not found in recipient's roster`);
          continue;
        }
        requestedSlots.push(slot);
      }

      // Swap: offered slots on proposer receive requested players (in order), extras become null
      // Requested slots on recipient receive offered players (in order), extras become null
      const proposerUpdates: Record<string, string | null> = {};
      const recipientUpdates: Record<string, string | null> = {};

      offeredSlots.forEach((slot, i) => {
        proposerUpdates[slot] = requestedPlayerIds[i] ?? null;
      });

      requestedSlots.forEach((slot, i) => {
        recipientUpdates[slot] = offeredPlayerIds[i] ?? null;
      });

      // Apply updates to proposer instance
      if (Object.keys(proposerUpdates).length > 0) {
        const setClauses = Object.keys(proposerUpdates).map((col, i) => `${col} = $${i + 3}`);
        await client.query(
          `UPDATE fantasydata.fantasy_team_instance
           SET ${setClauses.join(", ")}
           WHERE fantasy_team_id = $1 AND match_num = $2`,
          [proposer_fantasy_team_id, current_match_num, ...Object.values(proposerUpdates)]
        );
      }

      // Apply updates to recipient instance
      if (Object.keys(recipientUpdates).length > 0) {
        const setClauses = Object.keys(recipientUpdates).map((col, i) => `${col} = $${i + 3}`);
        await client.query(
          `UPDATE fantasydata.fantasy_team_instance
           SET ${setClauses.join(", ")}
           WHERE fantasy_team_id = $1 AND match_num = $2`,
          [recipient_fantasy_team_id, current_match_num, ...Object.values(recipientUpdates)]
        );
      }

      await client.query(
        `UPDATE fantasydata.trades SET status = 'completed' WHERE id = $1`,
        [tradeId]
      );

      await client.query("COMMIT");
      console.log("Trade executed successfully:", tradeId);
    } catch (err: any) {
      await client.query("ROLLBACK");
      console.error("TradeExecutor error for trade", tradeId, ":", err.message);
      throw err; // Let SQS retry
    } finally {
      client.release();
    }
  }
};
