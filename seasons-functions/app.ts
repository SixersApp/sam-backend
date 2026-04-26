import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   GET SEASON INFO (TEAMS + MATCHES)
   GET /seasons/:seasonId
   ======================================================================================= */
app.get("/seasons/:seasonId", async (req: Request, res: Response) => {
  const { seasonId } = req.params;

  if (!seasonId) {
    return res.status(400).json({
      message: "null or empty seasonId"
    });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      WITH
      season_info AS (
          SELECT s.id, s.tournament_id, s.start_year, s.end_year
          FROM irldata.season s
          WHERE s.id = $1
      ),

      -- Teams with their players
      team_players AS (
          SELECT
              t.id AS team_id,
              t.name AS team_name,
              t.image AS team_image,
              t.abbreviation AS team_abbreviation,
              json_agg(json_build_object(
                  'id', p.id,
                  'playerSeasonInfoId', psi.id,
                  'playerName', p.full_name,
                  'fullName', p.full_name,
                  'image', p.image,
                  'dateOfBirth', p.date_of_birth,
                  'role', psi.role,
                  'positionId', p.position_id,
                  'countryName', c.name,
                  'countryImage', c.image,
                  'rank', psi.rank,
                  'initialProjection', psi.initial_projection
              ) ORDER BY COALESCE(psi.rank, 999) ASC) AS players
          FROM irldata.player_season_info psi
          JOIN irldata.player p ON p.id = psi.player_id
          JOIN irldata.team t ON t.id = psi.team_id
          LEFT JOIN irldata.country_info c ON c.id = p.country_id
          WHERE psi.season_id = $1
            AND psi.tournament_id = (SELECT tournament_id FROM season_info)
          GROUP BY t.id, t.name, t.image, t.abbreviation
      ),

      -- Matches in this season
      season_matches AS (
          SELECT
              mi.id,
              mi.match_date,
              mi.status,
              mi.home_team_id,
              mi.away_team_id,
              mi.home_team_score,
              mi.away_team_score,
              mi.home_team_wickets,
              mi.away_team_wickets,
              mi.home_team_balls,
              mi.away_team_balls,
              mi.home_match_num,
              mi.away_match_num,
              mi.dls,
              mi.venue_id,
              ht.name AS home_team_name,
              ht.image AS home_team_image,
              ht.abbreviation AS home_team_abbreviation,
              at.name AS away_team_name,
              at.image AS away_team_image,
              at.abbreviation AS away_team_abbreviation
          FROM irldata.match_info mi
          JOIN irldata.team ht ON ht.id = mi.home_team_id
          JOIN irldata.team at ON at.id = mi.away_team_id
          WHERE mi.season_id = $1
            AND mi.tournament_id = (SELECT tournament_id FROM season_info)
          ORDER BY mi.match_date ASC
      )

      SELECT
          si.*,
          COALESCE(
              (SELECT json_agg(json_build_object(
                  'id', tp.team_id,
                  'name', tp.team_name,
                  'image', tp.team_image,
                  'abbreviation', tp.team_abbreviation,
                  'players', tp.players
              )) FROM team_players tp),
              '[]'::json
          ) AS teams,
          COALESCE(
              (SELECT json_agg(json_build_object(
                  'id', sm.id,
                  'matchDate', sm.match_date,
                  'status', sm.status,
                  'homeTeamId', sm.home_team_id,
                  'awayTeamId', sm.away_team_id,
                  'homeTeamScore', sm.home_team_score,
                  'awayTeamScore', sm.away_team_score,
                  'homeTeamWickets', sm.home_team_wickets,
                  'awayTeamWickets', sm.away_team_wickets,
                  'homeTeamBalls', sm.home_team_balls,
                  'awayTeamBalls', sm.away_team_balls,
                  'homeMatchNum', sm.home_match_num,
                  'awayMatchNum', sm.away_match_num,
                  'dls', sm.dls,
                  'venueId', sm.venue_id,
                  'homeTeamName', sm.home_team_name,
                  'homeTeamImage', sm.home_team_image,
                  'homeTeamAbbreviation', sm.home_team_abbreviation,
                  'awayTeamName', sm.away_team_name,
                  'awayTeamImage', sm.away_team_image,
                  'awayTeamAbbreviation', sm.away_team_abbreviation
              ) ORDER BY sm.match_date ASC) FROM season_matches sm),
              '[]'::json
          ) AS matches
      FROM season_info si;
    `;

    const result = await client.query(sql, [seasonId]);

    if ((result.rowCount ?? 0) === 0) {
      return res.status(204).json({
        message: "No rows were found that matched this season"
      });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /seasons/:seasonId failed:", err);
    return res.status(500).json({
      message: "Internal server error"
    });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET PLAYER HISTORICAL STATS ACROSS ALL SEASONS (WITH PER-SEASON PERCENTILES)
   GET /players/:playerId/stats
   ======================================================================================= */
app.get("/players/:playerId/stats", async (req: Request, res: Response) => {
  const { playerId } = req.params;

  if (!playerId) {
    return res.status(400).json({ message: "playerId is required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
WITH
-- All seasons/tournaments the player is registered in
player_seasons AS (
    SELECT psi.season_id, psi.tournament_id
    FROM irldata.player_season_info psi
    WHERE psi.player_id = $1
),

-- All player performances in finished matches across those seasons
all_performances AS (
    SELECT
        psi.player_id,
        psi.season_id,
        psi.tournament_id,
        pp.runs_scored,
        pp.balls_faced,
        pp.balls_bowled,
        pp.runs_conceded,
        pp.wickets_taken,
        pp.not_out
    FROM irldata.player_performance pp
    JOIN irldata.player_season_info psi ON psi.id = pp.player_season_id
    JOIN player_seasons ps ON ps.season_id = psi.season_id AND ps.tournament_id = psi.tournament_id
    JOIN irldata.match_info mi ON mi.id = pp.match_id AND mi.status = 'FINISHED'
),

-- Aggregate per player per season
player_aggregates AS (
    SELECT
        player_id,
        season_id,
        tournament_id,

        COUNT(*) FILTER (WHERE COALESCE(balls_faced, 0) > 0) AS matches_batted,
        COALESCE(SUM(runs_scored), 0) AS total_runs,
        COUNT(*) FILTER (WHERE runs_scored >= 50 AND runs_scored < 100) AS half_centuries,
        COUNT(*) FILTER (WHERE runs_scored >= 100) AS centuries,
        CASE WHEN SUM(balls_faced) > 0
            THEN ROUND((SUM(runs_scored) * 100.0 / SUM(balls_faced)), 2)
            ELSE NULL END AS strike_rate,
        CASE WHEN COUNT(*) FILTER (WHERE COALESCE(not_out, false) = false) > 0
            THEN ROUND((SUM(runs_scored)::numeric / COUNT(*) FILTER (WHERE COALESCE(not_out, false) = false)), 2)
            ELSE NULL END AS batting_average,

        COUNT(*) FILTER (WHERE COALESCE(balls_bowled, 0) > 0) AS matches_bowled,
        COALESCE(SUM(wickets_taken), 0) AS total_wickets,
        COUNT(*) FILTER (WHERE wickets_taken >= 3) AS three_wicket_hauls,
        COUNT(*) FILTER (WHERE wickets_taken >= 5) AS five_wicket_hauls,
        CASE WHEN SUM(wickets_taken) > 0
            THEN ROUND((SUM(runs_conceded)::numeric / SUM(wickets_taken)), 2)
            ELSE NULL END AS bowling_average,
        CASE WHEN SUM(balls_bowled) > 0
            THEN ROUND((SUM(runs_conceded) * 6.0 / SUM(balls_bowled)), 2)
            ELSE NULL END AS bowling_economy

    FROM all_performances
    GROUP BY player_id, season_id, tournament_id
),

-- Batting percentiles partitioned by season (only among players who batted)
batting_percentiles AS (
    SELECT
        player_id, season_id, tournament_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY matches_batted)::numeric, 4) AS matches_batted_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY total_runs)::numeric, 4) AS total_runs_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY half_centuries)::numeric, 4) AS half_centuries_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY centuries)::numeric, 4) AS centuries_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY strike_rate)::numeric, 4) AS strike_rate_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY batting_average)::numeric, 4) AS batting_average_pct
    FROM player_aggregates
    WHERE matches_batted > 0
),

-- Bowling percentiles partitioned by season (only among players who bowled)
bowling_percentiles AS (
    SELECT
        player_id, season_id, tournament_id,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY matches_bowled)::numeric, 4) AS matches_bowled_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY total_wickets)::numeric, 4) AS total_wickets_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY three_wicket_hauls)::numeric, 4) AS three_wicket_hauls_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY five_wicket_hauls)::numeric, 4) AS five_wicket_hauls_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY bowling_average DESC)::numeric, 4) AS bowling_average_pct,
        ROUND(PERCENT_RANK() OVER (PARTITION BY season_id, tournament_id ORDER BY bowling_economy DESC)::numeric, 4) AS bowling_economy_pct
    FROM player_aggregates
    WHERE matches_bowled > 0
)

SELECT
    psi.player_id,
    p.full_name,
    p.image,
    psi.role,
    t.name AS team_name,
    t.image AS team_image,
    t.abbreviation AS team_abbreviation,

    s.id AS season_id,
    s.end_year,
    ti.name AS tournament_name,
    ti.abbreviation AS tournament_abbreviation,

    COALESCE(pa.matches_batted, 0) AS matches_batted,
    COALESCE(pa.total_runs, 0) AS total_runs,
    COALESCE(pa.half_centuries, 0) AS half_centuries,
    COALESCE(pa.centuries, 0) AS centuries,
    pa.strike_rate,
    pa.batting_average,

    bp.matches_batted_pct,
    bp.total_runs_pct,
    bp.half_centuries_pct,
    bp.centuries_pct,
    bp.strike_rate_pct,
    bp.batting_average_pct,

    COALESCE(pa.matches_bowled, 0) AS matches_bowled,
    COALESCE(pa.total_wickets, 0) AS total_wickets,
    COALESCE(pa.three_wicket_hauls, 0) AS three_wicket_hauls,
    COALESCE(pa.five_wicket_hauls, 0) AS five_wicket_hauls,
    pa.bowling_average,
    pa.bowling_economy,

    bwp.matches_bowled_pct,
    bwp.total_wickets_pct,
    bwp.three_wicket_hauls_pct,
    bwp.five_wicket_hauls_pct,
    bwp.bowling_average_pct,
    bwp.bowling_economy_pct

FROM irldata.player_season_info psi
JOIN irldata.player p ON p.id = psi.player_id
JOIN irldata.season s ON s.id = psi.season_id
JOIN irldata.tournament_info ti ON ti.id = psi.tournament_id
LEFT JOIN irldata.team t ON t.id = psi.team_id
LEFT JOIN player_aggregates pa
    ON pa.player_id = psi.player_id
   AND pa.season_id = psi.season_id
   AND pa.tournament_id = psi.tournament_id
LEFT JOIN batting_percentiles bp
    ON bp.player_id = psi.player_id
   AND bp.season_id = psi.season_id
   AND bp.tournament_id = psi.tournament_id
LEFT JOIN bowling_percentiles bwp
    ON bwp.player_id = psi.player_id
   AND bwp.season_id = psi.season_id
   AND bwp.tournament_id = psi.tournament_id
WHERE psi.player_id = $1
ORDER BY s.end_year DESC;
    `;

    const result = await client.query(sql, [playerId]);

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Player not found" });
    }

    const first = result.rows[0];
    const seasons = result.rows.map((row: any) => ({
      seasonId: row.season_id,
      endYear: row.end_year,
      tournamentName: row.tournament_name,
      tournamentAbbreviation: row.tournament_abbreviation,
      teamName: row.team_name,
      teamImage: row.team_image,
      teamAbbreviation: row.team_abbreviation,
      role: row.role,
      batting: {
        matchesBatted: { value: row.matches_batted, percentile: row.matches_batted_pct },
        totalRuns: { value: row.total_runs, percentile: row.total_runs_pct },
        halfCenturies: { value: row.half_centuries, percentile: row.half_centuries_pct },
        centuries: { value: row.centuries, percentile: row.centuries_pct },
        strikeRate: { value: row.strike_rate, percentile: row.strike_rate_pct },
        battingAverage: { value: row.batting_average, percentile: row.batting_average_pct },
      },
      bowling: {
        matchesBowled: { value: row.matches_bowled, percentile: row.matches_bowled_pct },
        totalWickets: { value: row.total_wickets, percentile: row.total_wickets_pct },
        threeWicketHauls: { value: row.three_wicket_hauls, percentile: row.three_wicket_hauls_pct },
        fiveWicketHauls: { value: row.five_wicket_hauls, percentile: row.five_wicket_hauls_pct },
        bowlingAverage: { value: row.bowling_average, percentile: row.bowling_average_pct },
        bowlingEconomy: { value: row.bowling_economy, percentile: row.bowling_economy_pct },
      },
    }));

    return res.json({
      playerId: first.player_id,
      name: first.full_name,
      image: first.image,
      seasons,
    });

  } catch (err) {
    console.error("GET /players/:playerId/stats failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET ALL PLAYER PERFORMANCES FOR MOST RECENT SEASON
   GET /players/:playerId/performances
   ======================================================================================= */
app.get("/players/:playerId/performances", async (req: Request, res: Response) => {
  const { playerId } = req.params;
  const leagueId = req.query.leagueId as string | undefined;

  if (!playerId) {
    return res.status(400).json({ message: "playerId is required" });
  }

  if (!leagueId) {
    return res.status(400).json({ message: "leagueId is required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
WITH
league_ctx AS (
    SELECT l.season_id, l.tournament_id
    FROM fantasydata.leagues l
    WHERE l.id = $2
)

SELECT
    p.id AS player_id,
    p.full_name,
    p.image,
    psi.role,
    lc.season_id,
    s.end_year,
    ti.name AS tournament_name,
    ti.abbreviation AS tournament_abbreviation,
    t.name AS team_name,
    t.image AS team_image,
    t.abbreviation AS team_abbreviation,

    mi.id AS match_id,
    mi.match_date,
    mi.status AS match_status,
    ht.name AS home_team_name,
    ht.image AS home_team_image,
    ht.abbreviation AS home_team_abbreviation,
    at.name AS away_team_name,
    at.image AS away_team_image,
    at.abbreviation AS away_team_abbreviation,

    pp.runs_scored,
    pp.balls_faced,
    pp.fours,
    pp.sixes,
    pp.balls_bowled,
    pp.runs_conceded,
    pp.wickets_taken,
    pp.catches,
    pp.run_outs,
    pp.catches_dropped,
    pp.not_out

FROM irldata.player p
CROSS JOIN league_ctx lc
JOIN irldata.season s ON s.id = lc.season_id
JOIN irldata.tournament_info ti ON ti.id = lc.tournament_id
JOIN irldata.player_season_info psi
    ON psi.player_id = p.id
   AND psi.season_id = lc.season_id
   AND psi.tournament_id = lc.tournament_id
LEFT JOIN irldata.team t ON t.id = psi.team_id
LEFT JOIN irldata.player_performance pp ON pp.player_season_id = psi.id
LEFT JOIN irldata.match_info mi ON mi.id = pp.match_id
LEFT JOIN irldata.team ht ON ht.id = mi.home_team_id
LEFT JOIN irldata.team at ON at.id = mi.away_team_id
WHERE p.id = $1
ORDER BY mi.match_date ASC;
    `;

    const result = await client.query(sql, [playerId, leagueId]);

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Player not found" });
    }

    const first = result.rows[0];
    const performances = result.rows
      .filter((row: any) => row.match_id != null)
      .map((row: any) => ({
        matchId: row.match_id,
        matchDate: row.match_date,
        matchStatus: row.match_status,
        homeTeamName: row.home_team_name,
        homeTeamImage: row.home_team_image,
        homeTeamAbbreviation: row.home_team_abbreviation,
        awayTeamName: row.away_team_name,
        awayTeamImage: row.away_team_image,
        awayTeamAbbreviation: row.away_team_abbreviation,
        runsScored: row.runs_scored,
        ballsFaced: row.balls_faced,
        fours: row.fours,
        sixes: row.sixes,
        ballsBowled: row.balls_bowled,
        runsConceded: row.runs_conceded,
        wicketsTaken: row.wickets_taken,
        catches: row.catches,
        runOuts: row.run_outs,
        catchesDropped: row.catches_dropped,
        notOut: row.not_out,
      }));

    return res.json({
      playerId: first.player_id,
      name: first.full_name,
      image: first.image,
      role: first.role,
      seasonId: first.season_id,
      endYear: first.end_year,
      tournamentName: first.tournament_name,
      tournamentAbbreviation: first.tournament_abbreviation,
      teamName: first.team_name,
      teamImage: first.team_image,
      teamAbbreviation: first.team_abbreviation,
      performances,
    });

  } catch (err) {
    console.error("GET /players/:playerId/performances failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
