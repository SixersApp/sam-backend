import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

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
        0,
        0
      )
      RETURNING id;
    `, [match_data.match_date, match_data.tournament_id, match_data.season_id, match_data.venue_id, match_data.home_team_id, match_data.away_team_id, "NS"]);

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
      SET status = $1
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

    const match_data = (await client.query(`
      SELECT
        m.*,
        COALESCE(
          (
            SELECT jsonb_agg(
              to_jsonb(pp) || jsonb_build_object(
                'player_id', psi.player_id,
                'player_name', p.player_name,
                'full_name', p.full_name,
                'image', p.image,
                'date_of_birth', p.date_of_birth,
                'country_name', c.name,
                'country_image', c.image,
                'position_name', pos.name
              )
            )
            FROM irldata.player_performance pp
            JOIN irldata.player_season_info psi ON pp.player_season_id = psi.id
            JOIN irldata.player p ON psi.player_id = p.id
            LEFT JOIN irldata.country_info c ON p.country_id = c.id
            LEFT JOIN irldata.position pos ON p.position_id = pos.id
            WHERE pp.match_id = $1 AND pp.team_id = $2
          ),
          '[]'::jsonb
        ) AS home_team_players,

        COALESCE(
          (
            SELECT jsonb_agg(
              to_jsonb(pp) || jsonb_build_object(
                'player_id', psi.player_id,
                'player_name', p.player_name,
                'full_name', p.full_name,
                'image', p.image,
                'date_of_birth', p.date_of_birth,
                'country_name', c.name,
                'country_image', c.image,
                'position_name', pos.name
              )
            )
            FROM irldata.player_performance pp
            JOIN irldata.player_season_info psi ON pp.player_season_id = psi.id
            JOIN irldata.player p ON psi.player_id = p.id
            LEFT JOIN irldata.country_info c ON p.country_id = c.id
            LEFT JOIN irldata.position pos ON p.position_id = pos.id
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
    return res.json(match_data.rows[0]);

  } catch (e) {

    await client?.query('ROLLBACK');
    res.status(500).json({ message: "Something went wrong", data: e });

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

    const match_info = (await client.query(`
      SELECT * FROM irldata.match_info m
      WHERE m.id = $1;
    `, [ball_data.match_id])).rows[0];

    if (match_info == undefined) {
      return res.status(400).json({
        message: "matchup doesn't exist"
      });
    }

    if (match_info.tournament_id != tournamentId) {
      return res.status(401).json({
        message: "You do not have permission to score this match"
      })

    }

    if (match_info.status !== "LIVE") {
      return res.status(403).json({
        message: "Cannot score a match that is not currently LIVE"
      });
    }
    await client.query("BEGIN");

    const added_ball = (await client.query(`
      INSERT INTO irldata.ball_by_ball
      (
        match_id,
        batting_team,
        crease_batsman,
        run_batsman,
        bowler,
        runs_scored,
        balls_played,
        wicket_taken,
        wicket_type,
        catcher,
        run_outter,
        drop_catch,
        ball_num,
        four,
        six,
        extra_type
      ) VALUES
      (
        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
      )
      RETURNING *
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
    ])).rows[0];

    let updated_match_info;
    if ((ball_data.wicket_taken ?? null) == null) {
      const queryText = `
        WITH batter_update AS (
          UPDATE irldata.player_performance pp
          SET
            runs_scored = COALESCE(runs_scored, 0) + $1,
            balls_faced = COALESCE(balls_faced, 0) + $2,
            sixes = COALESCE(sixes, 0) + $3,
            fours = COALESCE(fours, 0) + $8
          FROM irldata.player_season_info psi
          WHERE pp.player_season_id = psi.id
            AND psi.player_id = $4
            AND pp.match_id = $5
        ),
        bowler_update AS (
          UPDATE irldata.player_performance pp
          SET
            runs_conceded = COALESCE(runs_conceded, 0) + $1,
            balls_bowled = COALESCE(balls_bowled, 0) + $2
          FROM irldata.player_season_info psi
          WHERE pp.player_season_id = psi.id
            AND psi.player_id = $6
            AND pp.match_id = $5
        )
        UPDATE irldata.match_info
        SET
          home_team_score = CASE
            WHEN home_team_id = $7 THEN COALESCE(home_team_score, 0) + $1
            ELSE home_team_score
          END,
          home_team_balls = CASE
            WHEN home_team_id = $7 THEN COALESCE(home_team_balls, 0) + $2
            ELSE home_team_balls
          END,

          away_team_score = CASE
            WHEN away_team_id = $7 THEN COALESCE(away_team_score, 0) + $1
            ELSE away_team_score
          END,
          away_team_balls = CASE
            WHEN away_team_id = $7 THEN COALESCE(away_team_balls, 0) + $2
            ELSE away_team_balls
          END
        WHERE id = $5
        RETURNING *;
      `;

      updated_match_info = (await client.query(queryText, [
        added_ball.runs_scored,
        added_ball.balls_played,
        (added_ball.six ?? false) ? 1 : 0,
        added_ball.crease_batsman,
        added_ball.match_id,
        added_ball.bowler,
        added_ball.batting_team,
        (added_ball.four ?? false) ? 1 : 0,
      ])).rows[0];
    } else if ((ball_data.wicket_type ?? null) == "CATCH") {
      const queryText = `
        WITH batter_update AS (
          UPDATE irldata.player_performance pp
          SET
            balls_faced = COALESCE(balls_faced, 0) + $1
          FROM irldata.player_season_info psi
          WHERE pp.player_season_id = psi.id
            AND psi.player_id = $3
            AND pp.match_id = $2
        ),
        bowler_update AS (
          UPDATE irldata.player_performance pp
          SET
            balls_bowled = COALESCE(balls_bowled, 0) + $1,
            wickets_taken = COALESCE(wickets_taken, 0) + 1
          FROM irldata.player_season_info psi
          WHERE pp.player_season_id = psi.id
            AND psi.player_id = $4
            AND pp.match_id = $2
        ),
        fielder_update AS (
          UPDATE irldata.player_performance pp
          SET
            catches = COALESCE(catches, 0) + 1
          FROM irldata.player_season_info psi
          WHERE pp.player_season_id = psi.id
            AND psi.player_id = $5
            AND pp.match_id = $2
        )
        UPDATE irldata.match_info
        SET
          home_team_balls = CASE
            WHEN home_team_id = $6 THEN COALESCE(home_team_balls, 0) + 1
            ELSE home_team_balls
          END,
          home_team_wickets = CASE
            WHEN home_team_id = $6 THEN COALESCE(home_team_wickets, 0) + 1
            ELSE home_team_wickets
          END,

          away_team_balls = CASE
            WHEN away_team_id = $6 THEN COALESCE(away_team_balls, 0) + 1
            ELSE away_team_balls
          END,
          away_team_wickets = CASE
            WHEN away_team_id = $6 THEN COALESCE(away_team_wickets, 0) + 1
            ELSE away_team_wickets
          END
        WHERE id = $2
        RETURNING *;
      `;

      updated_match_info = await client.query(queryText, [
        added_ball.balls_played,
        match_id,
        added_ball.crease_batsman,
        added_ball.bowler,
        added_ball.catcher,
        added_ball.batting_team
      ]);
    }

    const match_data = (await client.query(`
      SELECT
        m.*,

        COALESCE(
          (
            SELECT jsonb_agg(
              to_jsonb(pp) || jsonb_build_object(
                'player_id', psi.player_id,
                'player_name', p.player_name,
                'full_name', p.full_name,
                'image', p.image,
                'date_of_birth', p.date_of_birth,
                'country_name', c.name,
                'country_image', c.image,
                'position_name', pos.name
              )
            )
            FROM irldata.player_performance pp
            JOIN irldata.player_season_info psi ON pp.player_season_id = psi.id
            JOIN irldata.player p ON psi.player_id = p.id
            LEFT JOIN irldata.country_info c ON p.country_id = c.id
            LEFT JOIN irldata.position pos ON p.position_id = pos.id
            WHERE pp.match_id = $1 AND pp.team_id = $2
          ),
          '[]'::jsonb
        ) AS home_team_players,
        COALESCE(
          (
            SELECT jsonb_agg(
              to_jsonb(pp) || jsonb_build_object(
                'player_id', psi.player_id,
                'player_name', p.player_name,
                'full_name', p.full_name,
                'image', p.image,
                'date_of_birth', p.date_of_birth,
                'country_name', c.name,
                'country_image', c.image,
                'position_name', pos.name
              )
            )
            FROM irldata.player_performance pp
            JOIN irldata.player_season_info psi ON pp.player_season_id = psi.id
            JOIN irldata.player p ON psi.player_id = p.id
            LEFT JOIN irldata.country_info c ON p.country_id = c.id
            LEFT JOIN irldata.position pos ON p.position_id = pos.id
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
    `, [match_id, updated_match_info.home_team_id, updated_match_info.away_team_id])).rows[0];
    await client.query("COMMIT");
    return res.json(match_data);

  } catch (e) {

    await client?.query('ROLLBACK');
    res.status(500).json({ message: "Something went wrong", details: e });

  } finally {
    client?.release();
  }

});

export const lambdaHandler = createHandler(app);
