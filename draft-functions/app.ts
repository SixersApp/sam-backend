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

interface DraftOrderEntry {
  teamId: string;
  order: number;
}

interface DraftStartResult {
  leagueId: string;
  currentTeamId: string;
  currentRound: number;
  currentPick: number;
  pickExpiresAt: string;
  draftOrder: DraftOrderEntry[];
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

type LambdaResponse = DraftPick[] | DraftPickResult | DraftState | DraftStartResult;

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
            (pick_number - 1) / (SELECT COUNT(*) FROM fantasydata.fantasy_teams WHERE league_id = $1) + 1 as "roundNumber",
            pick_number as "pickNumber",
            league_id as "leagueId",
            pick_time as "timestamp"
          FROM fantasydata.draft_picks
          WHERE league_id = $1
          ORDER BY pick_number`,
          [leagueId]
        );

        return result.rows.map((row: any) => ({
          ...row,
          timestamp: row.timestamp?.toISOString() ?? new Date().toISOString(),
        }));
      }

      case 'getDraftState': {
        const { leagueId } = args;

        // Verify user is in the league
        const isMember = await verifyLeagueMembership(client, userId, leagueId);
        if (!isMember) {
          throw new Error('Forbidden: You are not a member of this league');
        }

        // Get league status
        const leagueResult = await client.query(
          `SELECT status FROM fantasydata.leagues WHERE id = $1`,
          [leagueId]
        );
        const leagueStatus = leagueResult.rows[0]?.status ?? 'draft_pending';

        // Get team count for round calculation
        const teamCountResult = await client.query(
          `SELECT COUNT(*) as count FROM fantasydata.fantasy_teams WHERE league_id = $1`,
          [leagueId]
        );
        const teamCount = parseInt(teamCountResult.rows[0].count, 10);

        // Get draft state from league_draft_state
        const stateResult = await client.query(
          `SELECT
            current_pick_number,
            current_fantasy_team_id,
            round_number,
            pick_deadline
          FROM fantasydata.league_draft_state
          WHERE league_id = $1`,
          [leagueId]
        );

        // Get all picks for this league
        const picksResult = await client.query(
          `SELECT
            fantasy_team_id as "teamId",
            player_id as "playerId",
            pick_number as "pickNumber",
            league_id as "leagueId",
            pick_time as "timestamp"
          FROM fantasydata.draft_picks
          WHERE league_id = $1
          ORDER BY pick_number`,
          [leagueId]
        );

        const picks: DraftPick[] = picksResult.rows.map((row: any) => ({
          ...row,
          roundNumber: teamCount > 0 ? Math.floor((row.pickNumber - 1) / teamCount) + 1 : 1,
          timestamp: row.timestamp?.toISOString() ?? new Date().toISOString(),
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
            status: leagueStatus === 'draft_in_progress' ? 'in_progress' : 'not_started',
          };
        }

        const state = stateResult.rows[0];
        return {
          leagueId,
          picks,
          currentTeamId: state.current_fantasy_team_id,
          currentRound: state.round_number,
          currentPick: state.current_pick_number,
          pickExpiresAt: state.pick_deadline?.toISOString() ?? null,
          status: leagueStatus === 'draft_in_progress' ? 'in_progress' :
                 leagueStatus === 'active' ? 'completed' : 'not_started',
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

        // Verify it's the user's team's turn
        const stateCheck = await client.query(
          `SELECT lds.current_fantasy_team_id, lds.current_pick_number, l.status
          FROM fantasydata.league_draft_state lds
          JOIN fantasydata.leagues l ON l.id = lds.league_id
          WHERE lds.league_id = $1`,
          [leagueId]
        );

        if (stateCheck.rowCount === 0) {
          throw new Error('Draft has not started for this league');
        }

        const currentState = stateCheck.rows[0];
        if (currentState.status !== 'draft_in_progress') {
          throw new Error('Draft is not in progress');
        }

        if (currentState.current_fantasy_team_id !== teamId) {
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
              (league_id, pick_number, fantasy_team_id, player_id)
            VALUES ($1, $2, $3, $4)
            RETURNING pick_time`,
            [leagueId, pickNumber, teamId, playerId]
          );

          const timestamp = pickResult.rows[0].pick_time?.toISOString() ?? new Date().toISOString();

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
          const nextPickNumber = pickNumber + 1;
          let nextRound = Math.floor((nextPickNumber - 1) / teamCount) + 1;
          let nextTeamId: string | null = null;

          // Determine if draft is complete (8 players per team)
          const totalPicks = teamCount * 8;

          if (nextPickNumber > totalPicks) {
            // Draft is complete
            await client.query(
              `UPDATE fantasydata.league_draft_state
              SET current_fantasy_team_id = NULL, pick_deadline = NULL
              WHERE league_id = $1`,
              [leagueId]
            );
            await client.query(
              `UPDATE fantasydata.leagues SET status = 'active' WHERE id = $1`,
              [leagueId]
            );
          } else {
            // Calculate next team based on snake draft or normal
            const pickInRound = ((nextPickNumber - 1) % teamCount) + 1;
            let nextTeamIndex: number;

            if (snakeDraft) {
              // Snake: odd rounds go 1->N, even rounds go N->1
              if (nextRound % 2 === 1) {
                nextTeamIndex = pickInRound - 1;
              } else {
                nextTeamIndex = teamCount - pickInRound;
              }
            } else {
              nextTeamIndex = pickInRound - 1;
            }

            nextTeamId = teams[nextTeamIndex]?.id ?? null;
            const nextPickDeadline = new Date(Date.now() + timePerPick * 1000);

            await client.query(
              `UPDATE fantasydata.league_draft_state
              SET round_number = $1, current_pick_number = $2, current_fantasy_team_id = $3, pick_deadline = $4
              WHERE league_id = $5`,
              [nextRound, nextPickNumber, nextTeamId, nextPickDeadline, leagueId]
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

          // Get the updated pick_deadline
          const updatedState = await client.query(
            `SELECT pick_deadline FROM fantasydata.league_draft_state WHERE league_id = $1`,
            [leagueId]
          );
          const nextPickExpiresAt = updatedState.rows[0]?.pick_deadline?.toISOString() ?? null;

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

      case 'startDraft': {
        const { input } = args;
        const { leagueId, draftOrder: inputDraftOrder } = input;

        // Verify user is in the league
        const isMember = await verifyLeagueMembership(client, userId, leagueId);
        if (!isMember) {
          throw new Error('Forbidden: You are not a member of this league');
        }

        // Verify user is the league creator
        const leagueCheck = await client.query(
          `SELECT creator_id, status FROM fantasydata.leagues WHERE id = $1`,
          [leagueId]
        );

        if (leagueCheck.rowCount === 0) {
          throw new Error('League not found');
        }

        const league = leagueCheck.rows[0];
        if (league.creator_id !== userId) {
          throw new Error('Forbidden: Only the league creator can start the draft');
        }

        if (league.status !== 'draft_pending') {
          throw new Error('Draft can only be started when league is in draft_pending status');
        }

        await client.query('BEGIN');

        try {
          // Get all teams in the league
          const teamsResult = await client.query(
            `SELECT id, created_at FROM fantasydata.fantasy_teams WHERE league_id = $1 ORDER BY created_at`,
            [leagueId]
          );
          const allTeams = teamsResult.rows;

          // Build draft order - use provided order for specified teams
          const specifiedTeamIds = new Set<string>();
          let maxSpecifiedOrder = 0;

          if (inputDraftOrder && Array.isArray(inputDraftOrder)) {
            for (const entry of inputDraftOrder) {
              await client.query(
                `UPDATE fantasydata.fantasy_teams SET draft_order = $1 WHERE id = $2 AND league_id = $3`,
                [entry.order, entry.teamId, leagueId]
              );
              specifiedTeamIds.add(entry.teamId);
              if (entry.order > maxSpecifiedOrder) {
                maxSpecifiedOrder = entry.order;
              }
            }
          }

          // Auto-assign order for unspecified teams
          let nextOrder = maxSpecifiedOrder + 1;
          for (const team of allTeams) {
            if (!specifiedTeamIds.has(team.id)) {
              await client.query(
                `UPDATE fantasydata.fantasy_teams SET draft_order = $1 WHERE id = $2`,
                [nextOrder, team.id]
              );
              nextOrder++;
            }
          }

          // Get draft settings
          const settingsResult = await client.query(
            `SELECT time_per_pick FROM fantasydata.league_draft_settings WHERE league_id = $1`,
            [leagueId]
          );
          const timePerPick = settingsResult.rows[0]?.time_per_pick ?? 60;

          // Get the first team (draft_order = 1)
          const firstTeamResult = await client.query(
            `SELECT id FROM fantasydata.fantasy_teams WHERE league_id = $1 ORDER BY draft_order LIMIT 1`,
            [leagueId]
          );
          const firstTeamId = firstTeamResult.rows[0]?.id;

          if (!firstTeamId) {
            throw new Error('No teams found in league');
          }

          const pickDeadline = new Date(Date.now() + timePerPick * 1000);

          // Create or update league_draft_state
          await client.query(
            `INSERT INTO fantasydata.league_draft_state
              (league_id, current_pick_number, current_fantasy_team_id, round_number, pick_deadline, direction)
            VALUES ($1, 1, $2, 1, $3, 'forward')
            ON CONFLICT (league_id) DO UPDATE SET
              current_pick_number = 1,
              current_fantasy_team_id = $2,
              round_number = 1,
              pick_deadline = $3,
              direction = 'forward'`,
            [leagueId, firstTeamId, pickDeadline]
          );

          // Update league status
          await client.query(
            `UPDATE fantasydata.leagues SET status = 'draft_in_progress' WHERE id = $1`,
            [leagueId]
          );

          await client.query('COMMIT');

          // Get final draft order
          const finalOrderResult = await client.query(
            `SELECT id as "teamId", draft_order as "order"
            FROM fantasydata.fantasy_teams
            WHERE league_id = $1
            ORDER BY draft_order`,
            [leagueId]
          );

          const draftOrder: DraftOrderEntry[] = finalOrderResult.rows;

          return {
            leagueId,
            currentTeamId: firstTeamId,
            currentRound: 1,
            currentPick: 1,
            pickExpiresAt: pickDeadline.toISOString(),
            draftOrder,
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
