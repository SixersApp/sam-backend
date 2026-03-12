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
  leagueId: string;
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

interface DraftEndResult {
  leagueId: string;
  status: string;
}

type LambdaResponse = DraftPick[] | DraftPickResult | DraftState | DraftStartResult | DraftEndResult;


// ── Hardcoded roster rules ─────────────────────────────────────────────

const POSITION_IDS = {
  BOWLING_ALLROUNDER: '61e23909-517c-4ee4-b46e-0ed59bd1f75f',
  WICKETKEEPER:       '836d3328-822d-4367-85b0-39ee48a4f5e6',
  BOWLER:             '8fb01a2b-3c72-4316-ad41-9f8231cf1a8a',
  ALLROUNDER:         'b4eb2689-ed30-459d-bbab-b09c6e7365fc',
  BATSMAN:            'd0dba3b8-bad7-46cc-81f5-bb117729abc5',
  BATTING_ALLROUNDER: 'e6338d95-1537-41b7-aa1b-50ff6eebc870',
  MIDDLE_ORDER_BATTER:'f3928ab5-6079-46c1-a431-9d3edb829da1',
  TOP_ORDER_BATTER:   'fd663301-878d-4021-9182-a457e2376d65',
};

const ALL_POSITION_IDS = Object.values(POSITION_IDS);

const BATSMAN_SLOT_INDEX = 0;

const ROSTER_SLOTS = [
  { name: 'Batsman',      max: 3, roles: [POSITION_IDS.BATSMAN, POSITION_IDS.TOP_ORDER_BATTER, POSITION_IDS.MIDDLE_ORDER_BATTER, POSITION_IDS.WICKETKEEPER] },
  { name: 'Bowler',        max: 3, roles: [POSITION_IDS.BOWLER] },
  { name: 'All-rounder',   max: 1, roles: [POSITION_IDS.ALLROUNDER, POSITION_IDS.BATTING_ALLROUNDER, POSITION_IDS.BOWLING_ALLROUNDER] },
  { name: 'Flex',          max: 1, roles: ALL_POSITION_IDS },
  { name: 'Bench',         max: 3, roles: ALL_POSITION_IDS },
];

/**
 * Simulates greedy slot assignment for a list of position IDs (in draft order).
 * Returns how many are in each slot and how many WKs are in the batsman slot.
 */
function assignToSlots(positionIds: string[]): { slotFilled: number[]; wkInBatsman: number } {
  const slotFilled = ROSTER_SLOTS.map(() => 0);
  let wkInBatsman = 0;

  for (const posId of positionIds) {
    for (let i = 0; i < ROSTER_SLOTS.length; i++) {
      if (ROSTER_SLOTS[i].roles.includes(posId) && slotFilled[i] < ROSTER_SLOTS[i].max) {
        slotFilled[i]++;
        if (i === BATSMAN_SLOT_INDEX && posId === POSITION_IDS.WICKETKEEPER) {
          wkInBatsman++;
        }
        break;
      }
    }
  }

  return { slotFilled, wkInBatsman };
}

/**
 * Validates whether a player with the given position can be added to a team
 * that already has the given list of drafted position IDs.
 * Returns null if valid, or an error message string if invalid.
 */
function validateRosterFit(existingPositionIds: string[], newPositionId: string): string | null {
  const before = assignToSlots(existingPositionIds);

  // Check if the new player fits in any slot
  let fitsSlotIndex = -1;
  for (let i = 0; i < ROSTER_SLOTS.length; i++) {
    if (ROSTER_SLOTS[i].roles.includes(newPositionId) && before.slotFilled[i] < ROSTER_SLOTS[i].max) {
      fitsSlotIndex = i;
      break;
    }
  }

  if (fitsSlotIndex === -1) {
    return 'No available roster slot for this player\'s position';
  }

  // Simulate with the new player added
  const after = assignToSlots([...existingPositionIds, newPositionId]);

  // WK constraint: if all 3 batsman slots would be full with 0 WKs, reject
  if (after.slotFilled[BATSMAN_SLOT_INDEX] >= ROSTER_SLOTS[BATSMAN_SLOT_INDEX].max && after.wkInBatsman === 0) {
    return 'At least one of the three batsman slots must be a wicketkeeper';
  }

  return null;
}

/**
 * Gets the ordered list of position IDs for a team's existing draft picks.
 */
async function getTeamDraftedPositions(client: any, leagueId: string, teamId: string): Promise<string[]> {
  const result = await client.query(
    `SELECT p.position_id
    FROM fantasydata.draft_picks dp
    JOIN irldata.player p ON p.id = dp.player_id
    WHERE dp.league_id = $1 AND dp.fantasy_team_id = $2
    ORDER BY dp.pick_number ASC`,
    [leagueId, teamId]
  );
  return result.rows.map((row: any) => row.position_id);
}

/**
 * Returns total roster capacity (sum of all slot max values).
 */
const TOTAL_ROSTER_SIZE = ROSTER_SLOTS.reduce((sum, slot) => sum + slot.max, 0);

/**
 * Checks if a team's roster is full (all slots filled).
 */
function isRosterFull(positionIds: string[]): boolean {
  const { slotFilled } = assignToSlots(positionIds);
  const totalFilled = slotFilled.reduce((sum, n) => sum + n, 0);
  return totalFilled >= TOTAL_ROSTER_SIZE;
}

/**
 * Checks if ALL teams in a league have full rosters.
 */
async function areAllRostersFull(client: any, leagueId: string): Promise<boolean> {
  const teamsResult = await client.query(
    `SELECT id FROM fantasydata.fantasy_teams WHERE league_id = $1`,
    [leagueId]
  );

  for (const team of teamsResult.rows) {
    const positions = await getTeamDraftedPositions(client, leagueId, team.id);
    if (!isRosterFull(positions)) {
      return false;
    }
  }

  return true;
}

/**
 * Ends the draft for a league: clears draft state and sets league status to active.
 */
async function performEndDraft(client: any, leagueId: string): Promise<void> {
  await client.query(
    `UPDATE fantasydata.leagues SET status = 'active' WHERE id = $1`,
    [leagueId]
  );
}

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


async function advanceDraftState(
  client: any,
  leagueId: string,
  pickNumber: number
): Promise<{ nextTeamId: string | null; nextPickDeadline: Date | null; draftComplete: boolean }> {
  // Check if all rosters are full
  const allFull = await areAllRostersFull(client, leagueId);

  if (allFull) {
    // Draft is complete
    await performEndDraft(client, leagueId);
    return { nextTeamId: null, nextPickDeadline: null, draftComplete: true };
  }

  // Get draft settings
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

  const nextPickNumber = pickNumber + 1;

  // Calculate next team based on snake draft or normal
  const nextRound = Math.floor((nextPickNumber - 1) / teamCount) + 1;
  const pickInRound = ((nextPickNumber - 1) % teamCount) + 1;
  let nextTeamIndex: number;

  if (snakeDraft) {
    if (nextRound % 2 === 1) {
      nextTeamIndex = pickInRound - 1;
    } else {
      nextTeamIndex = teamCount - pickInRound;
    }
  } else {
    nextTeamIndex = pickInRound - 1;
  }

  const nextTeamId = teams[nextTeamIndex]?.id ?? null;
  const nextPickDeadline = new Date(Date.now() + timePerPick * 1000);

  await client.query(
    `UPDATE fantasydata.league_draft_state
    SET round_number = $1, current_pick_number = $2, current_fantasy_team_id = $3, pick_deadline = $4
    WHERE league_id = $5`,
    [nextRound, nextPickNumber, nextTeamId, nextPickDeadline, leagueId]
  );

  return { nextTeamId, nextPickDeadline, draftComplete: false };
}

export const lambdaHandler = async (
  event: LambdaEvent
): Promise<LambdaResponse> => {
  console.log('Event:', JSON.stringify(event, null, 2));

  const { field, arguments: args, identity } = event;

  // autoPickDraftPlayer is called via IAM (no Cognito identity)
  const isSystemCall = field === 'autoPickDraftPlayer' || field === 'endDraft';
  const userId = identity?.sub;

  if (!isSystemCall && !userId) {
    throw new Error('Unauthorized: No user identity found');
  }

  const pool = getPool();
  const client = await pool.connect();

  try {
    switch (field) {
      case 'getDraftPicks': {
        const { leagueId } = args;

        const isMember = await verifyLeagueMembership(client, userId!, leagueId);
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

        const isMember = await verifyLeagueMembership(client, userId!, leagueId);
        if (!isMember) {
          throw new Error('Forbidden: You are not a member of this league');
        }

        const leagueResult = await client.query(
          `SELECT status FROM fantasydata.leagues WHERE id = $1`,
          [leagueId]
        );
        const leagueStatus = leagueResult.rows[0]?.status ?? 'draft_pending';

        const teamCountResult = await client.query(
          `SELECT COUNT(*) as count FROM fantasydata.fantasy_teams WHERE league_id = $1`,
          [leagueId]
        );
        const teamCount = parseInt(teamCountResult.rows[0].count, 10);

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

        const isMember = await verifyLeagueMembership(client, userId!, leagueId);
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

        // Validate roster fit
        const existingPositions = await getTeamDraftedPositions(client, leagueId, teamId);
        const playerPositionResult = await client.query(
          `SELECT position_id FROM irldata.player WHERE id = $1`,
          [playerId]
        );
        if (playerPositionResult.rowCount === 0) {
          throw new Error('Player not found');
        }
        const playerPositionId = playerPositionResult.rows[0].position_id;
        const rosterError = validateRosterFit(existingPositions, playerPositionId);
        if (rosterError) {
          throw new Error(rosterError);
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

          // Advance draft state
          const { nextTeamId, nextPickDeadline, draftComplete } = await advanceDraftState(client, leagueId, pickNumber);

          await client.query('COMMIT');

          const pick: DraftPick = {
            teamId,
            playerId,
            roundNumber,
            pickNumber,
            leagueId,
            timestamp,
          };

          return {
            pick,
            nextTeamId,
            nextPickExpiresAt: nextPickDeadline?.toISOString() ?? null,
            draftComplete,
            leagueId,
          };
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        }
      }

      case 'autoPickDraftPlayer': {
        const { input } = args;
        const { leagueId, expectedPickNumber } = input;

        // Verify draft is in progress and the pick hasn't been made yet
        const stateCheck = await client.query(
          `SELECT lds.current_fantasy_team_id, lds.current_pick_number, l.status, l.season_id, l.tournament_id
          FROM fantasydata.league_draft_state lds
          JOIN fantasydata.leagues l ON l.id = lds.league_id
          WHERE lds.league_id = $1`,
          [leagueId]
        );

        if (stateCheck.rowCount === 0) {
          throw new Error('Draft has not started for this league');
        }

        const draftState = stateCheck.rows[0];
        if (draftState.status !== 'draft_in_progress') {
          throw new Error('Draft is not in progress');
        }

        // If the current pick number doesn't match, someone already picked
        if (draftState.current_pick_number !== expectedPickNumber) {
          throw new Error('Pick already made, skipping autopick');
        }

        const teamId = draftState.current_fantasy_team_id;
        const { season_id, tournament_id } = draftState;

        // Get this team's existing drafted positions
        const existingPositions = await getTeamDraftedPositions(client, leagueId, teamId);

        // Find the best available player that passes roster validation
        const bestPlayerResult = await client.query(
          `SELECT psi.player_id, p.position_id
          FROM irldata.player_season_info psi
          JOIN irldata.player p ON p.id = psi.player_id
          WHERE psi.season_id = $1
            AND psi.tournament_id = $2
            AND psi.player_id NOT IN (
              SELECT player_id FROM fantasydata.draft_picks WHERE league_id = $3
            )
          ORDER BY psi.rank ASC`,
          [season_id, tournament_id, leagueId]
        );

        let playerId: string | null = null;
        for (const row of bestPlayerResult.rows) {
          const error = validateRosterFit(existingPositions, row.position_id);
          if (!error) {
            playerId = row.player_id;
            break;
          }
        }

        if (!playerId) {
          throw new Error('No available players to autopick that fit roster constraints');
        }

        // Get team count for round calculation
        const teamCountResult = await client.query(
          `SELECT COUNT(*) as count FROM fantasydata.fantasy_teams WHERE league_id = $1`,
          [leagueId]
        );
        const teamCount = parseInt(teamCountResult.rows[0].count, 10);
        const roundNumber = teamCount > 0 ? Math.floor((expectedPickNumber - 1) / teamCount) + 1 : 1;

        await client.query('BEGIN');

        try {
          // Insert the draft pick
          const pickResult = await client.query(
            `INSERT INTO fantasydata.draft_picks
              (league_id, pick_number, fantasy_team_id, player_id)
            VALUES ($1, $2, $3, $4)
            RETURNING pick_time`,
            [leagueId, expectedPickNumber, teamId, playerId]
          );

          const timestamp = pickResult.rows[0].pick_time?.toISOString() ?? new Date().toISOString();

          // Advance draft state
          const { nextTeamId, nextPickDeadline, draftComplete } = await advanceDraftState(client, leagueId, expectedPickNumber);

          await client.query('COMMIT');

          const pick: DraftPick = {
            teamId,
            playerId,
            roundNumber,
            pickNumber: expectedPickNumber,
            leagueId,
            timestamp,
          };

          return {
            pick,
            nextTeamId,
            nextPickExpiresAt: nextPickDeadline?.toISOString() ?? null,
            draftComplete,
            leagueId,
          };
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        }
      }

      case 'startDraft': {
        const { input } = args;
        const { leagueId, draftOrder: inputDraftOrder } = input;

        const isMember = await verifyLeagueMembership(client, userId!, leagueId);
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

          // Clear existing draft orders to avoid unique constraint violations
          await client.query(
            `UPDATE fantasydata.fantasy_teams SET draft_order = NULL WHERE league_id = $1`,
            [leagueId]
          );

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
              (league_id, current_pick_number, current_fantasy_team_id, round_number, pick_deadline)
            VALUES ($1, 1, $2, 1, $3)
            ON CONFLICT (league_id) DO UPDATE SET
              current_pick_number = 1,
              current_fantasy_team_id = $2,
              round_number = 1,
              pick_deadline = $3`,
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

      case 'endDraft': {
        const { leagueId } = args;

        // Verify the league exists and draft was in progress or is now active
        const leagueCheck = await client.query(
          `SELECT status FROM fantasydata.leagues WHERE id = $1`,
          [leagueId]
        );

        if (leagueCheck.rowCount === 0) {
          throw new Error('League not found');
        }

        const status = leagueCheck.rows[0].status;
        if (status !== 'active' && status !== 'draft_in_progress') {
          throw new Error('Draft is not in a completable state');
        }

        // If still in progress (race condition), end it now
        if (status === 'draft_in_progress') {
          await performEndDraft(client, leagueId);
        }

        return {
          leagueId,
          status: 'completed',
        };
      }

      default:
        throw new Error(`Unknown field: ${field}`);
    }
  } finally {
    client.release();
  }
};
