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


// ── Hardcoded roster rules (using psi.role from player_season_info) ───

const ROLES = {
  BATSMAN:       'Batsman',
  BOWLER:        'Bowler',
  ALL_ROUNDER:   'All-Rounder',
  WICKET_KEEPER: 'Wicket-Keeper',
};

const ALL_ROLES = Object.values(ROLES);

const BATSMAN_SLOT_INDEX = 0;

const ROSTER_SLOTS = [
  { name: 'Batsman',      max: 2, roles: [ROLES.BATSMAN, ROLES.WICKET_KEEPER] },
  { name: 'Bowler',        max: 3, roles: [ROLES.BOWLER] },
  { name: 'All-rounder',   max: 1, roles: [ROLES.ALL_ROUNDER] },
  { name: 'Wicket-keeper', max: 1, roles: [ROLES.WICKET_KEEPER] },
  { name: 'Flex',          max: 1, roles: ALL_ROLES },
  { name: 'Bench',         max: 3, roles: ALL_ROLES },
];

/**
 * Simulates greedy slot assignment for a list of roles (in draft order).
 * Returns how many are in each slot.
 */
function assignToSlots(roles: string[]): { slotFilled: number[] } {
  const slotFilled = ROSTER_SLOTS.map(() => 0);

  for (const role of roles) {
    for (let i = 0; i < ROSTER_SLOTS.length; i++) {
      if (ROSTER_SLOTS[i].roles.includes(role) && slotFilled[i] < ROSTER_SLOTS[i].max) {
        slotFilled[i]++;
        break;
      }
    }
  }

  return { slotFilled };
}

/**
 * Validates whether a player with the given role can be added to a team
 * that already has the given list of drafted roles.
 * Returns null if valid, or an error message string if invalid.
 */
function validateRosterFit(existingRoles: string[], newRole: string): string | null {
  const before = assignToSlots(existingRoles);

  // Check if the new player fits in any slot
  let fitsSlot = false;
  for (let i = 0; i < ROSTER_SLOTS.length; i++) {
    if (ROSTER_SLOTS[i].roles.includes(newRole) && before.slotFilled[i] < ROSTER_SLOTS[i].max) {
      fitsSlot = true;
      break;
    }
  }

  if (!fitsSlot) {
    return 'No available roster slot for this player\'s role';
  }

  return null;
}

/**
 * Gets the ordered list of roles for a team's existing draft picks.
 */
async function getTeamDraftedRoles(client: any, leagueId: string, teamId: string, tournamentId: string, seasonId: string): Promise<string[]> {
  const result = await client.query(
    `SELECT psi.role
    FROM fantasydata.draft_picks dp
    JOIN irldata.player_season_info psi
      ON psi.player_id = dp.player_id
     AND psi.tournament_id = $3
     AND psi.season_id = $4
    WHERE dp.league_id = $1 AND dp.fantasy_team_id = $2
    ORDER BY dp.pick_number ASC`,
    [leagueId, teamId, tournamentId, seasonId]
  );
  return result.rows.map((row: any) => row.role);
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
async function areAllRostersFull(client: any, leagueId: string, tournamentId: string, seasonId: string): Promise<boolean> {
  const teamsResult = await client.query(
    `SELECT id FROM fantasydata.fantasy_teams WHERE league_id = $1`,
    [leagueId]
  );

  for (const team of teamsResult.rows) {
    const roles = await getTeamDraftedRoles(client, leagueId, team.id, tournamentId, seasonId);
    if (!isRosterFull(roles)) {
      return false;
    }
  }

  return true;
}

// ── Roster column mapping ────────────────────────────────────────────

interface DraftedPlayer {
  player_id: string;
  role: string;
}

interface RosterAssignment {
  bat1: string | null;
  bat2: string | null;
  wicket1: string | null;
  bowl1: string | null;
  bowl2: string | null;
  bowl3: string | null;
  all1: string | null;
  flex1: string | null;
  bench1: string | null;
  bench2: string | null;
  bench3: string | null;
  captain: string | null;
  viceCaptain: string | null;
}

/**
 * Assigns drafted players to concrete roster columns.
 * Players should be ordered by rank (best first).
 * Uses the same greedy slot logic as validation, then maps to DB columns.
 */
function assignPlayersToRoster(players: DraftedPlayer[]): RosterAssignment {
  // Slot indices: 0=Batsman(2), 1=Bowler(3), 2=All-rounder(1), 3=Wicket-keeper(1), 4=Flex(1), 5=Bench(3)
  const slotPlayers: DraftedPlayer[][] = ROSTER_SLOTS.map(() => []);
  const activePlayerIds: string[] = [];

  for (const player of players) {
    for (let i = 0; i < ROSTER_SLOTS.length; i++) {
      if (ROSTER_SLOTS[i].roles.includes(player.role) && slotPlayers[i].length < ROSTER_SLOTS[i].max) {
        slotPlayers[i].push(player);
        if (i !== 5) { // not bench
          activePlayerIds.push(player.player_id);
        }
        break;
      }
    }
  }

  return {
    bat1: slotPlayers[0][0]?.player_id ?? null,
    bat2: slotPlayers[0][1]?.player_id ?? null,
    bowl1: slotPlayers[1][0]?.player_id ?? null,
    bowl2: slotPlayers[1][1]?.player_id ?? null,
    bowl3: slotPlayers[1][2]?.player_id ?? null,
    all1: slotPlayers[2][0]?.player_id ?? null,
    wicket1: slotPlayers[3][0]?.player_id ?? null,
    flex1: slotPlayers[4][0]?.player_id ?? null,
    bench1: slotPlayers[5][0]?.player_id ?? null,
    bench2: slotPlayers[5][1]?.player_id ?? null,
    bench3: slotPlayers[5][2]?.player_id ?? null,
    captain: activePlayerIds[0] ?? null,
    viceCaptain: activePlayerIds[1] ?? null,
  };
}

/**
 * Generates a round-robin schedule for the given teams over numWeeks weeks.
 * Uses the circle method. If odd team count, one team gets a bye each round.
 * Cycles through rounds if numWeeks > unique rounds.
 */
function generateRoundRobinSchedule(teamIds: string[], numWeeks: number): [string, string][][] {
  const n = teamIds.length;
  const roundsPerCycle = n - 1;

  // Generate all unique rounds using circle method (fix teams[0], rotate the rest)
  const rotating = teamIds.slice(1);
  const allRounds: [string, string][][] = [];

  for (let r = 0; r < roundsPerCycle; r++) {
    const current = [teamIds[0], ...rotating];
    const round: [string, string][] = [];

    for (let i = 0; i < n / 2; i++) {
      round.push([current[i], current[n - 1 - i]]);
    }

    allRounds.push(round);
    // Rotate: move last element to front
    rotating.unshift(rotating.pop()!);
  }

  // Fill weeks by cycling through rounds
  const schedule: [string, string][][] = [];
  for (let week = 0; week < numWeeks; week++) {
    schedule.push(allRounds[week % allRounds.length]);
  }

  return schedule;
}

/**
 * Ends the draft for a league:
 * 1. Sets league status to 'active'
 * 2. Calculates regular-season weeks (total weeks minus playoff weeks)
 * 3. Generates round-robin matchup schedule
 * 4. Creates fantasy_team_instance rows for every team/week with roster from draft picks
 * 5. Creates fantasy_matchups linking paired instances
 */
async function performEndDraft(client: any, leagueId: string): Promise<void> {
  // 1. Set league to active
  await client.query(
    `UPDATE fantasydata.leagues SET status = 'active' WHERE id = $1`,
    [leagueId]
  );

  // 2. Get tournament weeks and league teams (parallel)
  const [leagueInfo, teamsResult] = await Promise.all([
    client.query(
      `SELECT l.tournament_id, l.season_id, ti.weeks
       FROM fantasydata.leagues l
       JOIN irldata.tournament_info ti ON ti.id = l.tournament_id
       WHERE l.id = $1`,
      [leagueId]
    ),
    client.query(
      `SELECT id FROM fantasydata.fantasy_teams WHERE league_id = $1 ORDER BY draft_order`,
      [leagueId]
    ),
  ]);
  const { weeks: totalWeeks, season_id, tournament_id } = leagueInfo.rows[0];
  const teamIds: string[] = teamsResult.rows.map((r: any) => r.id);
  const teamCount = teamIds.length;

  // 3. Calculate playoff weeks and regular-season weeks
  const playoffWeeks = Math.ceil(Math.log2(teamCount));
  const regularWeeks = totalWeeks - playoffWeeks;

  // 4. Generate round-robin schedule
  const schedule = generateRoundRobinSchedule(teamIds, regularWeeks);

  // 5. Build roster assignment for each team from draft picks (single query)
  const allPicksResult = await client.query(
    `SELECT dp.fantasy_team_id, dp.player_id, psi.role
     FROM fantasydata.draft_picks dp
     JOIN irldata.player_season_info psi
       ON psi.player_id = dp.player_id
       AND psi.season_id = $2
       AND psi.tournament_id = $3
     WHERE dp.league_id = $1
     ORDER BY dp.fantasy_team_id, COALESCE(psi.rank, 999) ASC`,
    [leagueId, season_id, tournament_id]
  );

  const teamRosters: Record<string, RosterAssignment> = {};
  const picksByTeam: Record<string, DraftedPlayer[]> = {};
  for (const row of allPicksResult.rows) {
    if (!picksByTeam[row.fantasy_team_id]) {
      picksByTeam[row.fantasy_team_id] = [];
    }
    picksByTeam[row.fantasy_team_id].push({ player_id: row.player_id, role: row.role });
  }
  for (const teamId of teamIds) {
    teamRosters[teamId] = assignPlayersToRoster(picksByTeam[teamId] ?? []);
  }

  // 6. Create instances and matchups for each regular-season week (batched)
  for (let week = 1; week <= regularWeeks; week++) {
    // Batch-insert all team instances for this week
    const instanceValues: any[] = [];
    const instancePlaceholders: string[] = [];
    let paramIdx = 1;
    for (const teamId of teamIds) {
      const r = teamRosters[teamId];
      const placeholders = [`gen_random_uuid()`];
      for (let i = 0; i < 15; i++) {
        placeholders.push(`$${paramIdx++}`);
      }
      instancePlaceholders.push(`(${placeholders.join(',')})`);
      instanceValues.push(
        teamId, week, r.captain, r.viceCaptain,
        r.bat1, r.bat2, r.wicket1,
        r.bowl1, r.bowl2, r.bowl3,
        r.all1, r.flex1,
        r.bench1, r.bench2, r.bench3
      );
    }

    const insResult = await client.query(
      `INSERT INTO fantasydata.fantasy_team_instance
        (id, fantasy_team_id, match_num, captain, vice_captain,
         bat1, bat2, wicket1, bowl1, bowl2, bowl3, all1, flex1,
         bench1, bench2, bench3)
       VALUES ${instancePlaceholders.join(', ')}
       RETURNING id, fantasy_team_id`,
      instanceValues
    );

    const instanceIds: Record<string, string> = {};
    for (const row of insResult.rows) {
      instanceIds[row.fantasy_team_id] = row.id;
    }

    // Batch-insert all matchups for this week
    const weekMatchups = schedule[week - 1];
    if (weekMatchups.length > 0) {
      const matchupValues: any[] = [];
      const matchupPlaceholders: string[] = [];
      let mParamIdx = 1;
      for (const [t1, t2] of weekMatchups) {
        matchupPlaceholders.push(`(gen_random_uuid(), $${mParamIdx++}, $${mParamIdx++}, $${mParamIdx++}, $${mParamIdx++})`);
        matchupValues.push(leagueId, week, instanceIds[t1], instanceIds[t2]);
      }

      await client.query(
        `INSERT INTO fantasydata.fantasy_matchups
          (id, league_id, match_num, fantasy_team_instance1_id, fantasy_team_instance2_id)
         VALUES ${matchupPlaceholders.join(', ')}`,
        matchupValues
      );
    }
  }
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
  pickNumber: number,
  tournamentId: string,
  seasonId: string
): Promise<{ nextTeamId: string | null; nextPickDeadline: Date | null; draftComplete: boolean }> {
  // Check if all rosters are full
  const allFull = await areAllRostersFull(client, leagueId, tournamentId, seasonId);

  if (allFull) {
    // Draft is complete — don't call performEndDraft here.
    // The scheduler will queue an endDraft mutation via SQS,
    // which triggers the onDraftEnd subscription for clients.
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
          `SELECT lds.current_fantasy_team_id, lds.current_pick_number, l.status, l.tournament_id, l.season_id
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

        const { tournament_id: draftTournamentId, season_id: draftSeasonId } = currentState;

        // Validate roster fit
        const existingRoles = await getTeamDraftedRoles(client, leagueId, teamId, draftTournamentId, draftSeasonId);
        const playerRoleResult = await client.query(
          `SELECT psi.role FROM irldata.player_season_info psi
           WHERE psi.player_id = $1 AND psi.tournament_id = $2 AND psi.season_id = $3`,
          [playerId, draftTournamentId, draftSeasonId]
        );
        if (playerRoleResult.rowCount === 0) {
          throw new Error('Player not found in this tournament/season');
        }
        const playerRole = playerRoleResult.rows[0].role;
        const rosterError = validateRosterFit(existingRoles, playerRole);
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
          const { nextTeamId, nextPickDeadline, draftComplete } = await advanceDraftState(client, leagueId, pickNumber, draftTournamentId, draftSeasonId);

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

        // Get this team's existing drafted roles
        const existingRoles = await getTeamDraftedRoles(client, leagueId, teamId, tournament_id, season_id);

        // Find the best available player that passes roster validation
        const bestPlayerResult = await client.query(
          `SELECT psi.player_id, psi.role
          FROM irldata.player_season_info psi
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
          const error = validateRosterFit(existingRoles, row.role);
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
          const { nextTeamId, nextPickDeadline, draftComplete } = await advanceDraftState(client, leagueId, expectedPickNumber, tournament_id, season_id);

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
