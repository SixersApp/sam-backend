import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';

const app = createApp();

const sqsClient = new SQSClient({});
const MATCH_EVENTS_QUEUE_URL = process.env.MATCH_EVENTS_QUEUE_URL;
const MATCHUP_RESOLUTION_QUEUE_URL = process.env.MATCHUP_RESOLUTION_QUEUE_URL;

async function publishMatchEvent(tournamentId: string, event: any) {
  if (!MATCH_EVENTS_QUEUE_URL) return;

  await sqsClient.send(new SendMessageCommand({
    QueueUrl: MATCH_EVENTS_QUEUE_URL,
    MessageBody: JSON.stringify({
      channel: `/default/tournaments/${tournamentId}`,
      event,
    }),
  }));
}

app.post("/scoring/createMatch", async (req: Request, res: Response) => {
  const tournamentId = req.lambdaEvent.requestContext.authorizer?.claims?.["custom:tournamentId"];
  const match_data = JSON.parse(req.lambdaEvent.body ?? "{}");

  if (tournamentId == null) {
    return res.status(403).json({
      message: "no tournament id provided"
    });
  }

  const required_fields = ["tournament_id", "season_id", "home_team_id", "away_team_id", "match_date"];
  required_fields.forEach(field => {
    if (match_data[field] == undefined) {
      return res.status(403).json({
        message: `Field ${field} is required and missing from message body`
      })
    }
  });

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();
    const tournamentInfo = (await client.query(`
      SELECT * FROM irldata.tournament_info t
      WHERE t.id = $1;
    `, [match_data.tournament_id])).rows[0];
    const seasonInfo = (await client.query(`
      WITH season_teams
      AS (
        SELECT DISTINCT t.*
        FROM irldata.player_season_info psi
        JOIN irldata.team t ON psi.team_id = t.id
        WHERE psi.season_id = $1
      )
      SELECT
            s.*,
          (SELECT COALESCE(json_agg(season_teams.*), '[]') FROM season_teams) as teams
      FROM irldata.season s
      WHERE s.id = $1;

    `, [match_data.season_id])).rows[0];
    const awayTeamInfo = (await client.query(`
      SELECT t.* FROM irldata.team t WHERE t.id = $1;
    `, [match_data.away_team_id])).rows[0];
    const homeTeamInfo = (await client.query(`
      SELECT t.* FROM irldata.team t WHERE t.id = $1;
    `, [match_data.home_team_id])).rows[0];

    if (awayTeamInfo == undefined || homeTeamInfo == undefined) {
      return res.status(403).json({ message: "home team and/or away team doesn't exist" });
    }

    if (seasonInfo == undefined) {
      return res.status(403).json({ message: "season doesn't exist" });
    }

    if (tournamentInfo == undefined) {
      return res.status(403).json({ message: "tournament doesn't exist" });
    }

    if (seasonInfo.tournament_id != tournamentInfo.id) {
      return res.status(403).json({ message: "given season doesn't belong in given tournament" });
    }

    if (seasonInfo.teams.find((t: { id: string }) => t.id === homeTeamInfo.id) == undefined) {
      return res.status(403).json({ message: "home team doesn't exist in season" });
    }

    if (seasonInfo.teams.find((t: { id: string }) => t.id === awayTeamInfo.id) == undefined) {
      return res.status(403).json({ message: "away team doesn't exist in season" });
    }

    const homeMatchNum = (await client.query(`
      SELECT COALESCE(MAX(CASE WHEN home_team_id = $1 THEN home_match_num
                              WHEN away_team_id = $1 THEN away_match_num END), 0) + 1 AS next_num
      FROM irldata.match_info
      WHERE (home_team_id = $1 OR away_team_id = $1)
        AND season_id = $2 AND tournament_id = $3
    `, [match_data.home_team_id, match_data.season_id, match_data.tournament_id])).rows[0].next_num;

    const awayMatchNum = (await client.query(`
      SELECT COALESCE(MAX(CASE WHEN home_team_id = $1 THEN home_match_num
                              WHEN away_team_id = $1 THEN away_match_num END), 0) + 1 AS next_num
      FROM irldata.match_info
      WHERE (home_team_id = $1 OR away_team_id = $1)
        AND season_id = $2 AND tournament_id = $3
    `, [match_data.away_team_id, match_data.season_id, match_data.tournament_id])).rows[0].next_num;

    const response = await client.query(`
      INSERT INTO irldata.match_info
      (
        match_date,
        tournament_id,
        season_id,
        venue_id,
        home_team_id,
        away_team_id,
        inserted_at,
        status,
        home_match_num,
        away_match_num
      )
      VALUES
      (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        NOW(),
        $7,
        $8,
        $9
      )
      RETURNING id;
    `, [match_data.match_date, match_data.tournament_id, match_data.season_id, match_data.venue_id, match_data.home_team_id, match_data.away_team_id, "NS", homeMatchNum, awayMatchNum]);

    return res.json({ id: response.rows[0].id });
  } catch (e) {
    res.status(500).json({
      message: "Unexpected Error Occured"
    });
  } finally {
    client?.release();
  }


  res.json({
    userId: req.lambdaEvent.requestContext.authorizer?.claims?.["sub"],
    message: "Create Match!!"
  })
});

app.patch("/scoring/:matchId/startScoring", async (req: Request, res: Response) => {
  const tournamentId = req.lambdaEvent.requestContext.authorizer?.claims?.["custom:tournamentId"];
  const match_id = req.lambdaEvent.pathParameters?.matchId;
  const scoring_data = JSON.parse(req.lambdaEvent.body ?? "{}");

  const required_fields = ["batting_team", "crease_batsman", "run_batsman", "bowler"];
  for (const field of required_fields) {
    if (scoring_data[field] == undefined) {
      return res.status(400).json({
        message: `Field ${field} is required and missing from message body`
      });
    }
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const matchInfo = (await client.query(`
      SELECT * FROM irldata.match_info m
      WHERE m.id = $1;
    `, [match_id])).rows[0];

    if (matchInfo == undefined) {
      return res.status(403).json({ message: "Match doesn't exist" });
    }

    if (matchInfo.tournament_id !== tournamentId) {
      return res.status(401).json({ message: "You are not allowed to modify this match " });
    }

    await client.query("BEGIN");

    const updated_match = (await client.query(`
      UPDATE irldata.match_info
      SET status = $1, event_num = 1
      WHERE id = $2
      RETURNING *;
    `, ['LIVE', match_id])).rows[0];

    await client.query(`
      INSERT INTO irldata.player_performance (
        player_season_id, team_id, match_id, inserted_at
      )
      SELECT
        id, team_id, $1, NOW()
      FROM irldata.player_season_info
      WHERE season_id = $2 AND team_id IN ($3, $4)
      ON CONFLICT (player_season_id, match_id) DO NOTHING;
    `, [match_id, updated_match.season_id, updated_match.home_team_id, updated_match.away_team_id]);

    await client.query(`
      INSERT INTO irldata.ball_by_ball (
        match_id, batting_team, crease_batsman, run_batsman, bowler,
        runs_scored, balls_played, four, six, event_num
      ) VALUES ($1, $2, $3, $4, $5, 0, 0, false, false, 1)
    `, [match_id, scoring_data.batting_team, scoring_data.crease_batsman, scoring_data.run_batsman, scoring_data.bowler]);

    // Set not_out = true for the two opening batsmen
    await client.query(`
      UPDATE irldata.player_performance pp
      SET not_out = true
      FROM irldata.player_season_info psi
      WHERE pp.player_season_id = psi.id
        AND pp.match_id = $1
        AND psi.player_id IN ($2, $3)
    `, [match_id, scoring_data.crease_batsman, scoring_data.run_batsman]);

    const match_data = (await client.query(`
      SELECT
        m.*,
        COALESCE(
          (
            SELECT jsonb_agg(
              to_jsonb(pp) || jsonb_build_object(
                'player_id', psi.player_id,
                'player_name', p.full_name,
                'full_name', p.full_name,
                'image', p.image,
                'date_of_birth', p.date_of_birth,
                'country_name', c.name,
                'country_image', c.image,
                'role', psi.role
              )
            )
            FROM irldata.player_performance pp
            JOIN irldata.player_season_info psi ON pp.player_season_id = psi.id
            JOIN irldata.player p ON psi.player_id = p.id
            LEFT JOIN irldata.country_info c ON p.country_id = c.id
            WHERE pp.match_id = $1 AND pp.team_id = $2
          ),
          '[]'::jsonb
        ) AS home_team_players,

        COALESCE(
          (
            SELECT jsonb_agg(
              to_jsonb(pp) || jsonb_build_object(
                'player_id', psi.player_id,
                'player_name', p.full_name,
                'full_name', p.full_name,
                'image', p.image,
                'date_of_birth', p.date_of_birth,
                'country_name', c.name,
                'country_image', c.image,
                'role', psi.role
              )
            )
            FROM irldata.player_performance pp
            JOIN irldata.player_season_info psi ON pp.player_season_id = psi.id
            JOIN irldata.player p ON psi.player_id = p.id
            LEFT JOIN irldata.country_info c ON p.country_id = c.id
            WHERE pp.match_id = $1 AND pp.team_id = $3
          ),
          '[]'::jsonb
        ) AS away_team_players,

        COALESCE(
          (
            SELECT jsonb_agg(to_jsonb(bbb) ORDER BY bbb.ball_num ASC)
            FROM irldata.ball_by_ball bbb
            WHERE bbb.match_id = m.id
          ),
          '[]'::jsonb
        ) AS timeline

      FROM irldata.match_info m
      WHERE m.id = $1;
    `, [match_id, updated_match.home_team_id, updated_match.away_team_id]));
    await client.query(`COMMIT`);

    await publishMatchEvent(updated_match.tournament_id, {
      type: 'MATCH_STARTED',
      matchId: match_id,
      data: match_data.rows[0],
    });

    return res.json(match_data.rows[0]);

  } catch (e) {

    await client?.query('ROLLBACK');
    res.status(500).json({ message: "Something went wrong", data: e });

  } finally {
    client?.release();
  }

});

app.get("/scoring/:matchId", async (req: Request, res: Response) => {
  const match_id = req.lambdaEvent.pathParameters?.matchId;

  if (!match_id) {
    return res.status(400).json({ message: "Match ID is required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const matchInfo = (await client.query(`
      SELECT * FROM irldata.match_info m WHERE m.id = $1;
    `, [match_id])).rows[0];

    if (matchInfo == undefined) {
      return res.status(404).json({ message: "Match doesn't exist" });
    }

    const match_data = (await client.query(`
      SELECT
        m.*,
        COALESCE(
          (
            SELECT jsonb_agg(
              to_jsonb(pp) || jsonb_build_object(
                'player_id', psi.player_id,
                'player_name', p.full_name,
                'full_name', p.full_name,
                'image', p.image,
                'date_of_birth', p.date_of_birth,
                'country_name', c.name,
                'country_image', c.image,
                'role', psi.role
              )
            )
            FROM irldata.player_performance pp
            JOIN irldata.player_season_info psi ON pp.player_season_id = psi.id
            JOIN irldata.player p ON psi.player_id = p.id
            LEFT JOIN irldata.country_info c ON p.country_id = c.id
            WHERE pp.match_id = $1 AND pp.team_id = $2
          ),
          '[]'::jsonb
        ) AS home_team_players,

        COALESCE(
          (
            SELECT jsonb_agg(
              to_jsonb(pp) || jsonb_build_object(
                'player_id', psi.player_id,
                'player_name', p.full_name,
                'full_name', p.full_name,
                'image', p.image,
                'date_of_birth', p.date_of_birth,
                'country_name', c.name,
                'country_image', c.image,
                'role', psi.role
              )
            )
            FROM irldata.player_performance pp
            JOIN irldata.player_season_info psi ON pp.player_season_id = psi.id
            JOIN irldata.player p ON psi.player_id = p.id
            LEFT JOIN irldata.country_info c ON p.country_id = c.id
            WHERE pp.match_id = $1 AND pp.team_id = $3
          ),
          '[]'::jsonb
        ) AS away_team_players,

        COALESCE(
          (
            SELECT jsonb_agg(to_jsonb(bbb) ORDER BY bbb.event_num ASC)
            FROM irldata.ball_by_ball bbb
            WHERE bbb.match_id = m.id
          ),
          '[]'::jsonb
        ) AS timeline

      FROM irldata.match_info m
      WHERE m.id = $1;
    `, [match_id, matchInfo.home_team_id, matchInfo.away_team_id])).rows[0];

    return res.json(match_data);

  } catch (e) {
    console.error("GET /scoring/:matchId failed:", e);
    res.status(500).json({ message: "Something went wrong" });
  } finally {
    client?.release();
  }
});

app.post("/scoring/:matchId/addEvent", async (req: Request, res: Response) => {
  const tournamentId = req.lambdaEvent.requestContext.authorizer?.claims?.["custom:tournamentId"];
  const match_id = req.lambdaEvent.pathParameters?.matchId;
  const ball_data = req.body;

  const required_fields = ["match_id", "batting_team"];
  required_fields.forEach(f => {
    if (ball_data[f] == undefined) {
      return res.status(400).json({ message: "Missing match_id or batting_team id" });
    }
  });

  console.log("\n\n", ball_data, "\n\n");

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    await client.query("BEGIN");

    // Single query: validate match, compute next event_num, insert ball — 3 round trips → 1
    const insertResult = (await client.query(`
      WITH match AS (
        SELECT * FROM irldata.match_info WHERE id = $1
      ),
      next_event AS (
        SELECT COALESCE(MAX(event_num), 0) + 1 AS num
        FROM irldata.ball_by_ball WHERE match_id = $1
      ),
      inserted AS (
        INSERT INTO irldata.ball_by_ball (
          match_id, batting_team, crease_batsman, run_batsman, bowler,
          runs_scored, balls_played, wicket_taken, wicket_type, catcher,
          run_outter, drop_catch, ball_num, four, six, extra_type, event_num
        )
        SELECT
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
          (SELECT num FROM next_event)
        FROM match
        WHERE match.status = 'LIVE' AND match.tournament_id = $17
        RETURNING *
      )
      SELECT
        i.*,
        m.tournament_id AS m_tournament_id,
        m.home_team_id AS m_home_team_id,
        m.away_team_id AS m_away_team_id,
        m.status AS m_status
      FROM match m
      LEFT JOIN inserted i ON true;
    `, [
      match_id,
      ball_data.batting_team ?? null,
      ball_data.crease_batsman ?? null,
      ball_data.run_batsman ?? null,
      ball_data.bowler ?? null,
      ball_data.runs_scored ?? null,
      ball_data.balls_played ?? null,
      ball_data.wicket_taken ?? null,
      ball_data.wicket_type ?? null,
      ball_data.catcher ?? null,
      ball_data.run_outter ?? null,
      ball_data.drop_catch ?? null,
      ball_data.ball_num ?? null,
      ball_data.four ?? null,
      ball_data.six ?? null,
      ball_data.extra_type ?? null,
      tournamentId,
    ])).rows[0];

    if (!insertResult) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Match doesn't exist" });
    }
    if (!insertResult.id) {
      await client.query("ROLLBACK");
      if (insertResult.m_status !== 'LIVE') {
        return res.status(403).json({ message: "Cannot score a match that is not currently LIVE" });
      }
      return res.status(401).json({ message: "You do not have permission to score this match" });
    }

    const match_info = {
      tournament_id: insertResult.m_tournament_id,
      home_team_id: insertResult.m_home_team_id,
      away_team_id: insertResult.m_away_team_id,
    };
    const added_ball = insertResult;

    // ---- Derive everything from the event data ----
    const runsOffBat = added_ball.runs_scored ?? 0;
    const isFour = added_ball.four ?? false;
    const isSix = added_ball.six ?? false;
    const isWicket = added_ball.wicket_taken != null;
    const wicketType = added_ball.wicket_type ?? null;
    const extraType = added_ball.extra_type?.toUpperCase() ?? null;

    const isWide = extraType === 'WIDE';
    const isNoBall = extraType === 'NO_BALL';
    const isBye = extraType === 'BYE';
    const isLegBye = extraType === 'LEG_BYE';

    // Position-swap-only event: no delivery, no runs, no wicket — just batsmen crossing
    const ballsPlayed = added_ball.balls_played ?? 0;
    const isPositionSwap = runsOffBat === 0 && ballsPlayed === 0 && !isFour && !isSix && !isWicket && !extraType;
    if (isPositionSwap) {
      await client.query(`
        UPDATE irldata.match_info SET event_num = $1 WHERE id = $2;
      `, [added_ball.event_num, match_id]);

      await client.query("COMMIT");

      const eventPayload = {
        type: 'POSITION_SWAP',
        matchId: match_id,
        eventNum: added_ball.event_num,
        matchConfig: {
          batting_team: added_ball.batting_team,
          crease_batsman: added_ball.crease_batsman,
          run_batsman: added_ball.run_batsman,
          bowler: added_ball.bowler,
        },
      };

      await publishMatchEvent(match_info.tournament_id, eventPayload);

      return res.json({
        match_info: match_info,
        event: eventPayload,
      });
    }

    // Balls attribution
    // Wides/no-balls: not legal deliveries, batsman doesn't face
    // Byes/leg-byes: legal deliveries, batsman faces
    const batsmanBallsFaced = (isWide || isNoBall) ? 0 : 1;
    const bowlerLegalBalls = (isWide || isNoBall) ? 0 : 1;

    // Runs attribution
    // runs_scored = runs off the bat / runs actually run
    // Wides/no-balls add 1 extra run on top
    const extraRuns = (isWide || isNoBall) ? 1 : 0;
    const teamTotalRuns = runsOffBat + extraRuns;
    // Batsman gets credit only for runs off the bat (not byes, leg-byes, or wides)
    const batsmanRuns = (isBye || isLegBye || isWide) ? 0 : runsOffBat;
    // Bowler concedes everything except byes and leg-byes
    const bowlerRunsConceded = (isBye || isLegBye) ? 0 : teamTotalRuns;

    // Batsman swap: odd runs actually run = swap (boundaries don't cause swap naturally since 4/6 are even)
    const batsmenSwap = runsOffBat % 2 === 1;
    let newCreaseBatsman = added_ball.crease_batsman;
    let newRunBatsman = added_ball.run_batsman;
    if (batsmenSwap && !isWicket) {
      newCreaseBatsman = added_ball.run_batsman;
      newRunBatsman = added_ball.crease_batsman;
    }

    // ---- Collect all deltas per player, then execute one UPDATE each ----
    // This prevents edge cases like caught-and-bowled where the same player
    // is both bowler (wickets_taken, balls_bowled) and catcher (catches).
    const playerDeltas: Record<string, Record<string, number>> = {};

    function addDelta(playerId: string, stat: string, value: number) {
      if (!playerDeltas[playerId]) playerDeltas[playerId] = {};
      playerDeltas[playerId][stat] = (playerDeltas[playerId][stat] ?? 0) + value;
    }

    // Batter
    if (!isWide) {
      addDelta(added_ball.crease_batsman, 'balls_faced', batsmanBallsFaced);
      if (batsmanRuns > 0) addDelta(added_ball.crease_batsman, 'runs_scored', batsmanRuns);
      if (isFour) addDelta(added_ball.crease_batsman, 'fours', 1);
      if (isSix) addDelta(added_ball.crease_batsman, 'sixes', 1);
    }

    // Bowler
    if (bowlerLegalBalls > 0) addDelta(added_ball.bowler, 'balls_bowled', bowlerLegalBalls);
    if (bowlerRunsConceded > 0) addDelta(added_ball.bowler, 'runs_conceded', bowlerRunsConceded);
    if (isWicket && wicketType !== 'RUN_OUT') addDelta(added_ball.bowler, 'wickets_taken', 1);

    // Fielders (wicket scenarios)
    if (isWicket) {
      if (wicketType === 'CATCH' && added_ball.catcher) addDelta(added_ball.catcher, 'catches', 1);
      if (wicketType === 'RUN_OUT' && added_ball.run_outter) addDelta(added_ball.run_outter, 'run_outs', 1);
    }
    if (added_ball.drop_catch) addDelta(added_ball.drop_catch, 'catches_dropped', 1);

    // ---- Build all update queries, then execute in parallel ----
    // Player performance updates (each touches a different row, safe to parallelize)
    const performanceDeltas: any[] = [];
    const performancePromises: Promise<any>[] = [];

    for (const [playerId, changes] of Object.entries(playerDeltas)) {
      const stats = Object.entries(changes).filter(([_, v]) => v !== 0);
      if (stats.length === 0) continue;

      const setClauses: string[] = [];
      const params: any[] = [];
      let paramIdx = 0;

      for (const [stat, value] of stats) {
        paramIdx++;
        setClauses.push(`${stat} = COALESCE(${stat}, 0) + $${paramIdx}`);
        params.push(value);
      }

      paramIdx++;
      const playerIdParam = paramIdx;
      params.push(playerId);
      paramIdx++;
      const matchIdParam = paramIdx;
      params.push(match_id);

      performancePromises.push(
        client.query(`
          UPDATE irldata.player_performance pp
          SET ${setClauses.join(', ')}
          FROM irldata.player_season_info psi
          WHERE pp.player_season_id = psi.id
            AND psi.player_id = $${playerIdParam}
            AND pp.match_id = $${matchIdParam}
          RETURNING pp.id
        `, params).then(result => {
          if (result.rowCount === 0) {
            console.error(`Performance UPDATE matched 0 rows for player_id=${playerId}, match_id=${match_id}, changes=${JSON.stringify(changes)}`);
          }
          performanceDeltas.push({
            player_performance_id: result.rows[0]?.id,
            player_id: playerId,
            changes,
          });
        })
      );
    }

    // Match info update
    const isHome = match_info.home_team_id === added_ball.batting_team;
    const matchInfoDeltas: any = {};

    if (teamTotalRuns > 0) {
      matchInfoDeltas[isHome ? 'home_team_score' : 'away_team_score'] = teamTotalRuns;
    }
    if (bowlerLegalBalls > 0) {
      matchInfoDeltas[isHome ? 'home_team_balls' : 'away_team_balls'] = bowlerLegalBalls;
    }
    if (isWicket) {
      matchInfoDeltas[isHome ? 'home_team_wickets' : 'away_team_wickets'] = 1;
    }

    const matchInfoPromise = client.query(`
      UPDATE irldata.match_info
      SET
        home_team_score = COALESCE(home_team_score, 0) + $1,
        home_team_balls = COALESCE(home_team_balls, 0) + $2,
        home_team_wickets = COALESCE(home_team_wickets, 0) + $3,
        away_team_score = COALESCE(away_team_score, 0) + $4,
        away_team_balls = COALESCE(away_team_balls, 0) + $5,
        away_team_wickets = COALESCE(away_team_wickets, 0) + $6,
        event_num = $8
      WHERE id = $7
      RETURNING *;
    `, [
      matchInfoDeltas.home_team_score ?? 0,
      matchInfoDeltas.home_team_balls ?? 0,
      matchInfoDeltas.home_team_wickets ?? 0,
      matchInfoDeltas.away_team_score ?? 0,
      matchInfoDeltas.away_team_balls ?? 0,
      matchInfoDeltas.away_team_wickets ?? 0,
      match_id,
      added_ball.event_num,
    ]);

    // Run all updates in parallel — different rows/tables, no conflicts
    const [matchInfoResult] = await Promise.all([
      matchInfoPromise,
      ...performancePromises,
    ]);

    const updated_match_info = matchInfoResult.rows[0];

    // Set not_out = true for both batsmen at the crease (idempotent if already true)
    await client.query(`
      UPDATE irldata.player_performance pp
      SET not_out = true
      FROM irldata.player_season_info psi
      WHERE pp.player_season_id = psi.id
        AND pp.match_id = $1
        AND psi.player_id IN ($2, $3)
    `, [match_id, added_ball.crease_batsman, added_ball.run_batsman]);

    // Track not_out values for broadcast
    const notOutUpdates: Record<string, boolean | null> = {
      [added_ball.crease_batsman]: true,
      [added_ball.run_batsman]: true,
    };

    // If wicket: set not_out = false for the dismissed player
    if (isWicket) {
      await client.query(`
        UPDATE irldata.player_performance pp
        SET not_out = false
        FROM irldata.player_season_info psi
        WHERE pp.player_season_id = psi.id
          AND pp.match_id = $1
          AND psi.player_id = $2
      `, [match_id, added_ball.wicket_taken]);
      notOutUpdates[added_ball.wicket_taken] = false;
    }

    // Attach not_out to performanceDeltas
    for (const pd of performanceDeltas) {
      if (notOutUpdates[pd.player_id] !== undefined) {
        pd.not_out = notOutUpdates[pd.player_id];
      }
    }
    // Add entries for batsmen who had no stat deltas but got not_out updated
    for (const [playerId, notOut] of Object.entries(notOutUpdates)) {
      if (!performanceDeltas.find((pd: any) => pd.player_id === playerId)) {
        performanceDeltas.push({ player_id: playerId, not_out: notOut, changes: {} });
      }
    }

    await client.query("COMMIT");

    const eventPayload = {
      type: 'BALL_EVENT',
      matchId: match_id,
      eventNum: added_ball.event_num,
      matchConfig: {
        batting_team: added_ball.batting_team,
        crease_batsman: newCreaseBatsman,
        run_batsman: newRunBatsman,
        bowler: added_ball.bowler,
      },
      matchInfoDeltas,
      performanceDeltas,
    };

    await publishMatchEvent(match_info.tournament_id, eventPayload);

    return res.json({
      match_info: updated_match_info,
      event: eventPayload,
    });

  } catch (e) {

    await client?.query('ROLLBACK');
    res.status(500).json({ message: "Something went wrong", details: e });

  } finally {
    client?.release();
  }

});

app.post("/scoring/:matchId/undoEvent", async (req: Request, res: Response) => {
  const tournamentId = req.lambdaEvent.requestContext.authorizer?.claims?.["custom:tournamentId"];
  const match_id = req.lambdaEvent.pathParameters?.matchId;

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    await client.query("BEGIN");

    // Validate match exists, is LIVE, and belongs to this tournament
    const matchInfo = (await client.query(`
      SELECT * FROM irldata.match_info WHERE id = $1
    `, [match_id])).rows[0];

    if (!matchInfo) {
      await client.query("ROLLBACK");
      return res.status(400).json({ message: "Match doesn't exist" });
    }
    if (matchInfo.status !== 'LIVE') {
      await client.query("ROLLBACK");
      return res.status(403).json({ message: "Cannot undo events on a match that is not LIVE" });
    }
    if (matchInfo.tournament_id !== tournamentId) {
      await client.query("ROLLBACK");
      return res.status(401).json({ message: "You do not have permission to modify this match" });
    }

    const undoneEvents: any[] = [];
    let ballUndoMatchInfoDeltas: any = null;
    let ballUndoPerformanceDeltas: any[] = [];
    let ballEventIndex = -1;
    let keepUndoing = true;

    while (keepUndoing) {
      // Fetch the last event for this match
      const lastBall = (await client.query(`
        SELECT * FROM irldata.ball_by_ball
        WHERE match_id = $1
        ORDER BY event_num DESC
        LIMIT 1
      `, [match_id])).rows[0];

      if (!lastBall) {
        await client.query("ROLLBACK");
        return res.status(400).json({ message: "No events to undo" });
      }

      // Don't allow undoing the initial config event (event_num = 1 from startScoring)
      if (lastBall.event_num <= 1) {
        await client.query("ROLLBACK");
        return res.status(400).json({ message: "Cannot undo the initial match configuration event" });
      }

      // Derive deltas using the same logic as addEvent
      const runsOffBat = lastBall.runs_scored ?? 0;
      const isFour = lastBall.four ?? false;
      const isSix = lastBall.six ?? false;
      const isWicket = lastBall.wicket_taken != null;
      const wicketType = lastBall.wicket_type ?? null;
      const extraType = lastBall.extra_type?.toUpperCase() ?? null;

      const isWide = extraType === 'WIDE';
      const isNoBall = extraType === 'NO_BALL';
      const isBye = extraType === 'BYE';
      const isLegBye = extraType === 'LEG_BYE';

      const ballsPlayed = lastBall.balls_played ?? 0;
      const isPositionSwap = runsOffBat === 0 && ballsPlayed === 0 && !isFour && !isSix && !isWicket && !extraType;

      // Delete the ball_by_ball row
      await client.query(`
        DELETE FROM irldata.ball_by_ball WHERE id = $1
      `, [lastBall.id]);

      if (isPositionSwap) {
        undoneEvents.push({
          type: 'UNDO_POSITION_SWAP',
          matchId: match_id,
          eventNum: lastBall.event_num,
          matchConfig: {
            batting_team: lastBall.batting_team,
            crease_batsman: lastBall.crease_batsman,
            run_batsman: lastBall.run_batsman,
            bowler: lastBall.bowler,
          },
        });
        // Config event — keep undoing the previous event too
        continue;
      }

      // Not a position swap — compute and reverse all deltas
      const batsmanBallsFaced = (isWide || isNoBall) ? 0 : 1;
      const bowlerLegalBalls = (isWide || isNoBall) ? 0 : 1;

      const extraRuns = (isWide || isNoBall) ? 1 : 0;
      const teamTotalRuns = runsOffBat + extraRuns;
      const batsmanRuns = (isBye || isLegBye || isWide) ? 0 : runsOffBat;
      const bowlerRunsConceded = (isBye || isLegBye) ? 0 : teamTotalRuns;

      // Build negative player deltas
      const playerDeltas: Record<string, Record<string, number>> = {};

      function addDelta(playerId: string, stat: string, value: number) {
        if (!playerDeltas[playerId]) playerDeltas[playerId] = {};
        playerDeltas[playerId][stat] = (playerDeltas[playerId][stat] ?? 0) + value;
      }

      // Batter (negate)
      if (!isWide) {
        addDelta(lastBall.crease_batsman, 'balls_faced', -batsmanBallsFaced);
        if (batsmanRuns > 0) addDelta(lastBall.crease_batsman, 'runs_scored', -batsmanRuns);
        if (isFour) addDelta(lastBall.crease_batsman, 'fours', -1);
        if (isSix) addDelta(lastBall.crease_batsman, 'sixes', -1);
      }

      // Bowler (negate)
      if (bowlerLegalBalls > 0) addDelta(lastBall.bowler, 'balls_bowled', -bowlerLegalBalls);
      if (bowlerRunsConceded > 0) addDelta(lastBall.bowler, 'runs_conceded', -bowlerRunsConceded);
      if (isWicket && wicketType !== 'RUN_OUT') addDelta(lastBall.bowler, 'wickets_taken', -1);

      // Fielders (negate)
      if (isWicket) {
        if (wicketType === 'CATCH' && lastBall.catcher) addDelta(lastBall.catcher, 'catches', -1);
        if (wicketType === 'RUN_OUT' && lastBall.run_outter) addDelta(lastBall.run_outter, 'run_outs', -1);
      }
      if (lastBall.drop_catch) addDelta(lastBall.drop_catch, 'catches_dropped', -1);

      // Execute player performance updates
      const performanceDeltas: any[] = [];
      const performancePromises: Promise<any>[] = [];

      for (const [playerId, changes] of Object.entries(playerDeltas)) {
        const stats = Object.entries(changes).filter(([_, v]) => v !== 0);
        if (stats.length === 0) continue;

        const setClauses: string[] = [];
        const params: any[] = [];
        let paramIdx = 0;

        for (const [stat, value] of stats) {
          paramIdx++;
          setClauses.push(`${stat} = COALESCE(${stat}, 0) + $${paramIdx}`);
          params.push(value);
        }

        paramIdx++;
        const playerIdParam = paramIdx;
        params.push(playerId);
        paramIdx++;
        const matchIdParam = paramIdx;
        params.push(match_id);

        performancePromises.push(
          client.query(`
            UPDATE irldata.player_performance pp
            SET ${setClauses.join(', ')}
            FROM irldata.player_season_info psi
            WHERE pp.player_season_id = psi.id
              AND psi.player_id = $${playerIdParam}
              AND pp.match_id = $${matchIdParam}
            RETURNING pp.id
          `, params).then(result => {
            performanceDeltas.push({
              player_performance_id: result.rows[0]?.id,
              player_id: playerId,
              changes,
            });
          })
        );
      }

      // Reverse match info score deltas (event_num set once after the loop)
      const isHome = matchInfo.home_team_id === lastBall.batting_team;
      const matchInfoDeltas: any = {};

      if (teamTotalRuns > 0) {
        matchInfoDeltas[isHome ? 'home_team_score' : 'away_team_score'] = -teamTotalRuns;
      }
      if (bowlerLegalBalls > 0) {
        matchInfoDeltas[isHome ? 'home_team_balls' : 'away_team_balls'] = -bowlerLegalBalls;
      }
      if (isWicket) {
        matchInfoDeltas[isHome ? 'home_team_wickets' : 'away_team_wickets'] = -1;
      }

      await Promise.all([
        client.query(`
          UPDATE irldata.match_info
          SET
            home_team_score = COALESCE(home_team_score, 0) + $1,
            home_team_balls = COALESCE(home_team_balls, 0) + $2,
            home_team_wickets = COALESCE(home_team_wickets, 0) + $3,
            away_team_score = COALESCE(away_team_score, 0) + $4,
            away_team_balls = COALESCE(away_team_balls, 0) + $5,
            away_team_wickets = COALESCE(away_team_wickets, 0) + $6
          WHERE id = $7
        `, [
          matchInfoDeltas.home_team_score ?? 0,
          matchInfoDeltas.home_team_balls ?? 0,
          matchInfoDeltas.home_team_wickets ?? 0,
          matchInfoDeltas.away_team_score ?? 0,
          matchInfoDeltas.away_team_balls ?? 0,
          matchInfoDeltas.away_team_wickets ?? 0,
          match_id,
        ]),
        ...performancePromises,
      ]);

      // Handle not_out reversal
      const undoNotOutUpdates: Record<string, boolean | null> = {};

      // If we undid a wicket, restore not_out = true for the dismissed player
      if (isWicket) {
        await client.query(`
          UPDATE irldata.player_performance pp
          SET not_out = true
          FROM irldata.player_season_info psi
          WHERE pp.player_season_id = psi.id
            AND pp.match_id = $1
            AND psi.player_id = $2
        `, [match_id, lastBall.wicket_taken]);
        undoNotOutUpdates[lastBall.wicket_taken] = true;
      }

      // If batsmen's balls_faced is now 0, they haven't batted — set not_out = null
      const batsmenToCheck = [lastBall.crease_batsman, lastBall.run_batsman].filter(Boolean);
      if (batsmenToCheck.length > 0) {
        await client.query(`
          UPDATE irldata.player_performance pp
          SET not_out = NULL
          FROM irldata.player_season_info psi
          WHERE pp.player_season_id = psi.id
            AND pp.match_id = $1
            AND psi.player_id = ANY($2)
            AND COALESCE(pp.balls_faced, 0) = 0
        `, [match_id, batsmenToCheck]);

        // Check which batsmen actually got set to null
        const checkResult = await client.query(`
          SELECT psi.player_id, pp.not_out
          FROM irldata.player_performance pp
          JOIN irldata.player_season_info psi ON psi.id = pp.player_season_id
          WHERE pp.match_id = $1 AND psi.player_id = ANY($2)
        `, [match_id, batsmenToCheck]);

        for (const row of checkResult.rows) {
          if (row.not_out === null) {
            undoNotOutUpdates[row.player_id] = null;
          }
        }
      }

      // Attach not_out to performanceDeltas
      for (const pd of performanceDeltas) {
        if (undoNotOutUpdates[pd.player_id] !== undefined) {
          pd.not_out = undoNotOutUpdates[pd.player_id];
        }
      }
      for (const [playerId, notOut] of Object.entries(undoNotOutUpdates)) {
        if (!performanceDeltas.find((pd: any) => pd.player_id === playerId)) {
          performanceDeltas.push({ player_id: playerId, not_out: notOut, changes: {} });
        }
      }

      ballUndoMatchInfoDeltas = matchInfoDeltas;
      ballUndoPerformanceDeltas = performanceDeltas;

      // Reserve the UNDO_BALL_EVENT slot in the correct position
      // (matchConfig and eventNum filled in after the loop with the final restored state)
      ballEventIndex = undoneEvents.length;
      undoneEvents.push(null);

      // Check if the now-latest event is an over-change config that should also be undone.
      // An over-change config is a position swap where the bowler differs from the event before it.
      // Handle it directly here instead of via the loop to avoid undoing extra ball events.
      keepUndoing = false;

      const nowLatest = (await client.query(`
        SELECT * FROM irldata.ball_by_ball
        WHERE match_id = $1
        ORDER BY event_num DESC
        LIMIT 1
      `, [match_id])).rows[0];

      if (nowLatest && nowLatest.event_num > 1) {
        const nlRunsOffBat = nowLatest.runs_scored ?? 0;
        const nlBallsPlayed = nowLatest.balls_played ?? 0;
        const nlIsConfig = nlRunsOffBat === 0 && nlBallsPlayed === 0
          && !(nowLatest.four ?? false) && !(nowLatest.six ?? false)
          && nowLatest.wicket_taken == null && !nowLatest.extra_type;

        if (nlIsConfig) {
          // Compare bowler to the event before this config
          const beforeConfig = (await client.query(`
            SELECT bowler FROM irldata.ball_by_ball
            WHERE match_id = $1 AND event_num < $2
            ORDER BY event_num DESC
            LIMIT 1
          `, [match_id, nowLatest.event_num])).rows[0];

          if (!beforeConfig || nowLatest.bowler !== beforeConfig.bowler) {
            // Over-change config — undo it directly and stop
            await client.query(`DELETE FROM irldata.ball_by_ball WHERE id = $1`, [nowLatest.id]);
            undoneEvents.push({
              type: 'UNDO_POSITION_SWAP',
              matchId: match_id,
              eventNum: nowLatest.event_num,
              matchConfig: {
                batting_team: nowLatest.batting_team,
                crease_batsman: nowLatest.crease_batsman,
                run_batsman: nowLatest.run_batsman,
                bowler: nowLatest.bowler,
              },
            });
          }
        }
      }
    }

    // All deletions done — set final event_num and get restored state
    const finalLatest = (await client.query(`
      SELECT * FROM irldata.ball_by_ball
      WHERE match_id = $1
      ORDER BY event_num DESC
      LIMIT 1
    `, [match_id])).rows[0];

    const finalEventNum = finalLatest?.event_num ?? 0;

    const updated_match_info = (await client.query(`
      UPDATE irldata.match_info SET event_num = $1 WHERE id = $2 RETURNING *
    `, [finalEventNum, match_id])).rows[0];

    // Fill in the UNDO_BALL_EVENT with the final restored config
    undoneEvents[ballEventIndex] = {
      type: 'UNDO_BALL_EVENT',
      matchId: match_id,
      eventNum: finalEventNum,
      matchConfig: {
        batting_team: finalLatest?.batting_team ?? null,
        crease_batsman: finalLatest?.crease_batsman ?? null,
        run_batsman: finalLatest?.run_batsman ?? null,
        bowler: finalLatest?.bowler ?? null,
      },
      matchInfoDeltas: ballUndoMatchInfoDeltas,
      performanceDeltas: ballUndoPerformanceDeltas,
    };

    await client.query("COMMIT");

    // Publish all undo events in order
    for (const event of undoneEvents) {
      await publishMatchEvent(matchInfo.tournament_id, event);
    }

    return res.json({
      match_info: updated_match_info,
      undoneEvents,
    });

  } catch (e) {
    await client?.query('ROLLBACK');
    res.status(500).json({ message: "Something went wrong", details: e });
  } finally {
    client?.release();
  }
});

app.patch("/scoring/:matchId/endMatch", async (req: Request, res: Response) => {
  const tournamentId = req.lambdaEvent.requestContext.authorizer?.claims?.["custom:tournamentId"];
  const match_id = req.lambdaEvent.pathParameters?.matchId;

  let client;
  try {
    const pool = getPool();
    client = await pool.connect();

    const matchInfo = (await client.query(`
      SELECT * FROM irldata.match_info WHERE id = $1
    `, [match_id])).rows[0];

    if (!matchInfo) {
      return res.status(400).json({ message: "Match doesn't exist" });
    }
    if (matchInfo.status !== 'LIVE') {
      return res.status(403).json({ message: "Cannot end a match that is not currently LIVE" });
    }
    if (matchInfo.tournament_id !== tournamentId) {
      return res.status(401).json({ message: "You do not have permission to modify this match" });
    }

    const updated_match = (await client.query(`
      UPDATE irldata.match_info
      SET status = 'FINISHED'
      WHERE id = $1
      RETURNING *;
    `, [match_id])).rows[0];

    await publishMatchEvent(matchInfo.tournament_id, {
      type: 'MATCH_ENDED',
      matchId: match_id,
      eventNum: updated_match.event_num,
      matchInfoDeltas: {
        status: 'FINISHED',
      },
    });

    // Trigger matchup resolution
    if (MATCHUP_RESOLUTION_QUEUE_URL) {
      await sqsClient.send(new SendMessageCommand({
        QueueUrl: MATCHUP_RESOLUTION_QUEUE_URL,
        MessageBody: JSON.stringify({
          matchId: match_id,
          seasonId: matchInfo.season_id,
          tournamentId: matchInfo.tournament_id,
        }),
      }));
    }

    return res.json(updated_match);

  } catch (e) {
    console.error("PATCH /scoring/:matchId/endMatch failed:", e);
    res.status(500).json({ message: "Something went wrong", details: e });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
