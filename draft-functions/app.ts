import { getPool } from "/opt/nodejs/index";

interface DraftPick {
  teamId: string;
  playerId: string;
  roundNumber: number;
  pickNumber: number;
  leagueId: string;
  timestamp: string;
}

interface DraftPickResult {
  pick: DraftPick;
  nextTeamId: string | null;
  nextPickExpiresAt: string | null;
}

interface DraftState {
  leagueId: string;
  picks: DraftPick[];
  currentTeamId: string | null;
  currentRound: number;
  currentPick: number;
  pickExpiresAt: string | null;
  status: string;
}

interface AppSyncIdentity {
  sub: string;
  username?: string;
  claims?: Record<string, any>;
}

interface LambdaEvent {
  field: string;
  arguments: Record<string, any>;
  identity?: AppSyncIdentity;
}

type LambdaResponse = DraftPick[] | DraftPickResult | DraftState;

/**
 * Verifies user is a member of the league by checking if they have a fantasy team in it
 */
async function verifyLeagueMembership(
  client: any,
  userId: string,
  leagueId: string
): Promise<boolean> {
  const result = await client.query(
    `SELECT 1 FROM fantasydata.fantasy_teams WHERE league_id = $1 AND user_id = $2`,
    [leagueId, userId]
  );
  return (result.rowCount ?? 0) > 0;
}

export const lambdaHandler = async (
  event: LambdaEvent
): Promise<LambdaResponse> => {
  console.log('Event:', JSON.stringify(event, null, 2));

  const { field, arguments: args, identity } = event;
  const userId = identity?.sub;

  if (!userId) {
    throw new Error('Unauthorized: No user identity found');
  }

  const pool = getPool();
  const client = await pool.connect();

  try {
    switch (field) {
      case 'getDraftPicks': {
        const { leagueId } = args;

        // Verify user is in the league
        const isMember = await verifyLeagueMembership(client, userId, leagueId);
        if (!isMember) {
          throw new Error('Forbidden: You are not a member of this league');
        }

        const result = await client.query(
          `SELECT
            fantasy_team_id as "teamId",
            player_id as "playerId",
            round_number as "roundNumber",
            pick_number as "pickNumber",
            league_id as "leagueId",
            picked_at as "timestamp"
          FROM fantasydata.draft_picks
          WHERE league_id = $1
          ORDER BY round_number, pick_number`,
          [leagueId]
        );

        return result.rows.map((row: any) => ({
          ...row,
          timestamp: row.timestamp.toISOString(),
        }));
      }

      case 'getDraftState': {
        const { leagueId } = args;

        // Verify user is in the league
        const isMember = await verifyLeagueMembership(client, userId, leagueId);
        if (!isMember) {
          throw new Error('Forbidden: You are not a member of this league');
        }

        // Get draft state
        const stateResult = await client.query(
          `SELECT
            current_round,
            current_pick,
            current_team_id,
            pick_expires_at,
            status
          FROM fantasydata.draft_state
          WHERE league_id = $1`,
          [leagueId]
        );

        // Get all picks for this league
        const picksResult = await client.query(
          `SELECT
            fantasy_team_id as "teamId",
            player_id as "playerId",
            round_number as "roundNumber",
            pick_number as "pickNumber",
            league_id as "leagueId",
            picked_at as "timestamp"
          FROM fantasydata.draft_picks
          WHERE league_id = $1
          ORDER BY round_number, pick_number`,
          [leagueId]
        );

        const picks: DraftPick[] = picksResult.rows.map((row: any) => ({
          ...row,
          timestamp: row.timestamp.toISOString(),
        }));

        // If no draft state exists, return default state
        if (stateResult.rowCount === 0) {
          return {
            leagueId,
            picks,
            currentTeamId: null,
            currentRound: 1,
            currentPick: 1,
            pickExpiresAt: null,
            status: 'not_started',
          };
        }

        const state = stateResult.rows[0];
        return {
          leagueId,
          picks,
          currentTeamId: state.current_team_id,
          currentRound: state.current_round,
          currentPick: state.current_pick,
          pickExpiresAt: state.pick_expires_at?.toISOString() ?? null,
          status: state.status,
        };
      }

      case 'postDraftPick': {
        const { input } = args;
        const { teamId, playerId, roundNumber, pickNumber, leagueId } = input;

        // Verify user is in the league
        const isMember = await verifyLeagueMembership(client, userId, leagueId);
        if (!isMember) {
          throw new Error('Forbidden: You are not a member of this league');
        }

        // Verify it's the user's team's turn (check current_team_id in draft_state)
        const stateCheck = await client.query(
          `SELECT current_team_id, status FROM fantasydata.draft_state WHERE league_id = $1`,
          [leagueId]
        );

        if (stateCheck.rowCount === 0) {
          throw new Error('Draft has not started for this league');
        }

        const currentState = stateCheck.rows[0];
        if (currentState.status !== 'in_progress') {
          throw new Error('Draft is not in progress');
        }

        if (currentState.current_team_id !== teamId) {
          throw new Error('It is not your turn to pick');
        }

        // Verify the user owns this team
        const teamOwnerCheck = await client.query(
          `SELECT user_id FROM fantasydata.fantasy_teams WHERE id = $1`,
          [teamId]
        );

        if (teamOwnerCheck.rowCount === 0 || teamOwnerCheck.rows[0].user_id !== userId) {
          throw new Error('Forbidden: You do not own this team');
        }

        await client.query('BEGIN');

        try {
          // Insert the draft pick
          const pickResult = await client.query(
            `INSERT INTO fantasydata.draft_picks
              (league_id, fantasy_team_id, player_id, round_number, pick_number)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING picked_at`,
            [leagueId, teamId, playerId, roundNumber, pickNumber]
          );

          const timestamp = pickResult.rows[0].picked_at.toISOString();

          // Get draft settings for time calculation
          const settingsResult = await client.query(
            `SELECT time_per_pick, snake_draft FROM fantasydata.league_draft_settings WHERE league_id = $1`,
            [leagueId]
          );
          const settings = settingsResult.rows[0];
          const timePerPick = settings?.time_per_pick ?? 60;
          const snakeDraft = settings?.snake_draft ?? true;

          // Get team count and draft order
          const teamsResult = await client.query(
            `SELECT id, draft_order FROM fantasydata.fantasy_teams
            WHERE league_id = $1
            ORDER BY draft_order`,
            [leagueId]
          );
          const teams = teamsResult.rows;
          const teamCount = teams.length;

          // Calculate next pick
          let nextRound = roundNumber;
          let nextPick = pickNumber + 1;
          let nextTeamId: string | null = null;

          // Determine if draft is complete (assuming 8 rounds for now - could be configurable)
          const totalRounds = 8;

          if (pickNumber >= teamCount) {
            // Move to next round
            nextRound = roundNumber + 1;
            nextPick = 1;
          }

          if (nextRound > totalRounds) {
            // Draft is complete
            await client.query(
              `UPDATE fantasydata.draft_state
              SET status = 'completed', current_team_id = NULL, pick_expires_at = NULL
              WHERE league_id = $1`,
              [leagueId]
            );
            await client.query(
              `UPDATE fantasydata.leagues SET status = 'active' WHERE id = $1`,
              [leagueId]
            );
          } else {
            // Calculate next team based on snake draft or normal
            let nextTeamIndex: number;
            if (snakeDraft) {
              // Snake: odd rounds go 1->N, even rounds go N->1
              if (nextRound % 2 === 1) {
                nextTeamIndex = nextPick - 1;
              } else {
                nextTeamIndex = teamCount - nextPick;
              }
            } else {
              nextTeamIndex = (nextPick - 1) % teamCount;
            }

            nextTeamId = teams[nextTeamIndex]?.id ?? null;
            const nextPickExpiresAt = new Date(Date.now() + timePerPick * 1000);

            await client.query(
              `UPDATE fantasydata.draft_state
              SET current_round = $1, current_pick = $2, current_team_id = $3, pick_expires_at = $4
              WHERE league_id = $5`,
              [nextRound, nextPick, nextTeamId, nextPickExpiresAt, leagueId]
            );
          }

          await client.query('COMMIT');

          const pick: DraftPick = {
            teamId,
            playerId,
            roundNumber,
            pickNumber,
            leagueId,
            timestamp,
          };

          // Get the updated pick_expires_at
          const updatedState = await client.query(
            `SELECT pick_expires_at FROM fantasydata.draft_state WHERE league_id = $1`,
            [leagueId]
          );
          const nextPickExpiresAt = updatedState.rows[0]?.pick_expires_at?.toISOString() ?? null;

          return {
            pick,
            nextTeamId,
            nextPickExpiresAt,
          };
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        }
      }

      default:
        throw new Error(`Unknown field: ${field}`);
    }
  } finally {
    client.release();
  }
};
