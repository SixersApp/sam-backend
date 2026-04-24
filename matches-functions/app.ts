import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   GET ALL LIVE / UPCOMING MATCHES FOR USER (HOME FEED)
   GET /matches/feed
   ======================================================================================= */
app.get("/matches/feed", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      SELECT
    json_build_object(
        'tournament_id', t.id,
        'tournament_name', t.name,
        'abbreviation', t.abbreviation,
        'weeks', t.weeks,
        'matches', COALESCE(
            (
                SELECT json_agg(
                    json_build_object(
                        'id', m.id,
                        'match_date', m.match_date,
                        'season_id', m.season_id,
                        'venue_id', m.venue_id,
                        'home_team_id', m.home_team_id,
                        'away_team_id', m.away_team_id,
                        'home_team_score', m.home_team_score,
                        'away_team_score', m.away_team_score,
                        'home_team_wickets', m.home_team_wickets,
                        'away_team_wickets', m.away_team_wickets,
                        'home_team_balls', m.home_team_balls,
                        'away_team_balls', m.away_team_balls,
                        'dls', m.dls,
                        'status', m.status,
                        'home_team_name', ht.name,
                        'home_team_image', ht.image,
                        'home_team_abbreviation', ht.abbreviation,
                        'away_team_name', at.name,
                        'away_team_image', at.image,
                        'away_team_abbreviation', at.abbreviation
                    ) ORDER BY
                        CASE WHEN m.status = 'Live' THEN 0 ELSE 1 END,
                        m.match_date ASC
                )
                FROM irldata.match_info m
                JOIN irldata.team ht ON ht.id = m.home_team_id
                JOIN irldata.team at ON at.id = m.away_team_id
                WHERE m.tournament_id = t.id -- Correlate to the outer tournament
                AND m.match_date >= NOW()
                AND m.match_date <= NOW() + INTERVAL '1 week'
            ),
            '[]'::json -- Return an empty array if no matches found
        )
    ) AS tournament_data
FROM irldata.tournament_info t
WHERE t.id IN (
    SELECT DISTINCT l.tournament_id
    FROM fantasydata.fantasy_teams ft
    JOIN fantasydata.leagues l ON l.id = ft.league_id
    WHERE ft.user_id = $1
);
    `;

    const result = await client.query(sql, [userId]);

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /matches/feed failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET ALL RELEVANT IRL MATCH INFO FOR THE USER'S ACTIVE FANTASY MATCHUPS
   GET /matches/active
   ======================================================================================= */
app.get("/matches/active", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      WITH
      -- 1. User's active leagues
      user_leagues AS (
          SELECT DISTINCT l.id AS league_id, l.season_id, l.tournament_id
          FROM fantasydata.fantasy_teams ft
          JOIN fantasydata.leagues l ON l.id = ft.league_id
          WHERE ft.user_id = $1
            AND l.status = 'active'
      ),

      -- 2. All matchups in user's active leagues
      league_matchups AS (
          SELECT m.id AS matchup_id, m.match_num, m.league_id,
                 m.fantasy_team_instance1_id, m.fantasy_team_instance2_id,
                 l.season_id, l.tournament_id
          FROM fantasydata.fantasy_matchups m
          JOIN user_leagues ul ON ul.league_id = m.league_id
          JOIN fantasydata.leagues l ON l.id = m.league_id
      ),

      -- 3. Distinct IRL teams in each matchup's starting lineup
      matchup_irl_teams AS (
          SELECT DISTINCT lm.matchup_id, lm.match_num, lm.league_id,
                 lm.season_id, lm.tournament_id, psi.team_id
          FROM league_matchups lm
          JOIN fantasydata.fantasy_team_instance ti1 ON ti1.id = lm.fantasy_team_instance1_id
          JOIN fantasydata.fantasy_team_instance ti2 ON ti2.id = lm.fantasy_team_instance2_id
          CROSS JOIN LATERAL (VALUES
              (ti1.bat1), (ti1.bat2), (ti1.wicket1),
              (ti1.bowl1), (ti1.bowl2), (ti1.bowl3),
              (ti1.all1), (ti1.flex1),
              (ti2.bat1), (ti2.bat2), (ti2.wicket1),
              (ti2.bowl1), (ti2.bowl2), (ti2.bowl3),
              (ti2.all1), (ti2.flex1)
          ) AS u(player_id)
          JOIN irldata.player_season_info psi
              ON psi.player_id = u.player_id
             AND psi.season_id = lm.season_id
             AND psi.tournament_id = lm.tournament_id
          WHERE u.player_id IS NOT NULL
      ),

      -- 4. IRL match statuses for each matchup's roster players
      matchup_match_statuses AS (
          SELECT DISTINCT mt.matchup_id, mt.league_id, mt.match_num,
                 mt.tournament_id, mt.season_id, mi.status
          FROM matchup_irl_teams mt
          JOIN irldata.match_info mi
              ON mi.tournament_id = mt.tournament_id
             AND mi.season_id = mt.season_id
             AND (
                  (mi.home_team_id = mt.team_id AND mi.home_match_num = mt.match_num)
                  OR (mi.away_team_id = mt.team_id AND mi.away_match_num = mt.match_num)
             )
      ),

      -- 5. Active match_nums per league
      active_matchup_weeks AS (
          SELECT DISTINCT league_id, match_num, tournament_id, season_id
          FROM matchup_match_statuses
          GROUP BY matchup_id, league_id, match_num, tournament_id, season_id
          HAVING
              COUNT(*) FILTER (WHERE status = 'LIVE') > 0
              OR (
                  COUNT(*) FILTER (WHERE status IN ('FINISHED', 'ABAN.')) > 0
                  AND COUNT(*) FILTER (WHERE status IN ('NS', 'LIVE')) > 0
              )
      ),

      -- 6. Completed weeks (all IRL matches finished) for fallback
      completed_weeks AS (
          SELECT DISTINCT mms.league_id, mms.match_num, mms.tournament_id, mms.season_id
          FROM matchup_match_statuses mms
          GROUP BY mms.matchup_id, mms.league_id, mms.match_num, mms.tournament_id, mms.season_id
          HAVING COUNT(*) FILTER (WHERE status IN ('NS', 'LIVE')) = 0
      ),

      -- 7. Target weeks: active + next, OR fallback to next after latest completed
      target_weeks AS (
          SELECT league_id, match_num, tournament_id, season_id
          FROM active_matchup_weeks
          UNION
          SELECT sub.league_id, sub.next_num, sub.tournament_id, sub.season_id
          FROM (
              SELECT league_id, tournament_id, season_id, MAX(match_num) + 1 AS next_num
              FROM active_matchup_weeks
              GROUP BY league_id, tournament_id, season_id
          ) sub
          WHERE EXISTS (
              SELECT 1 FROM fantasydata.fantasy_matchups fm
              WHERE fm.league_id = sub.league_id AND fm.match_num = sub.next_num
          )
          UNION
          -- Fallback: no active weeks, show next game after latest completed
          SELECT lc.league_id, lc.max_completed + 1, lc.tournament_id, lc.season_id
          FROM (
              SELECT league_id, tournament_id, season_id, MAX(match_num) AS max_completed
              FROM completed_weeks
              GROUP BY league_id, tournament_id, season_id
          ) lc
          WHERE NOT EXISTS (SELECT 1 FROM active_matchup_weeks aw WHERE aw.league_id = lc.league_id)
            AND EXISTS (
              SELECT 1 FROM fantasydata.fantasy_matchups fm
              WHERE fm.league_id = lc.league_id AND fm.match_num = lc.max_completed + 1
          )
      ),

      -- 7. Distinct target match_nums per tournament/season
      target_match_nums AS (
          SELECT DISTINCT tournament_id, season_id, match_num
          FROM target_weeks
      ),

      -- 8. All IRL matches where either team's match_num is a target
      relevant_matches AS (
          SELECT DISTINCT mi.*
          FROM target_match_nums tmn
          JOIN irldata.match_info mi
              ON mi.tournament_id = tmn.tournament_id
             AND mi.season_id = tmn.season_id
             AND (
                  mi.home_match_num = tmn.match_num
                  OR mi.away_match_num = tmn.match_num
             )
      )

      -- 9. Final: matches with player performances
      SELECT
          rm.id,
          rm.match_date,
          rm.tournament_id,
          rm.season_id,
          rm.venue_id,
          rm.home_team_id,
          rm.away_team_id,
          rm.home_team_score,
          rm.away_team_score,
          rm.home_team_wickets,
          rm.away_team_wickets,
          rm.home_team_balls,
          rm.away_team_balls,
          rm.home_match_num,
          rm.away_match_num,
          rm.dls,
          rm.status,
          rm.event_num,
          t.name AS tournament_name,
          t.abbreviation,
          t.weeks,
          ht.name AS home_team_name,
          ht.image AS home_team_image,
          ht.abbreviation AS home_team_abbreviation,
          at.name AS away_team_name,
          at.image AS away_team_image,
          at.abbreviation AS away_team_abbreviation,
          COALESCE(
              (
                  SELECT json_agg(json_build_object(
                      'player_performance_id', pp.id,
                      'player_id', psi.player_id,
                      'team_id', pp.team_id,
                      'runs_scored', pp.runs_scored,
                      'balls_faced', pp.balls_faced,
                      'fours', pp.fours,
                      'sixes', pp.sixes,
                      'balls_bowled', pp.balls_bowled,
                      'runs_conceded', pp.runs_conceded,
                      'wickets_taken', pp.wickets_taken,
                      'catches', pp.catches,
                      'run_outs', pp.run_outs,
                      'catches_dropped', pp.catches_dropped,
                      'not_out', pp.not_out
                  ))
                  FROM irldata.player_performance pp
                  JOIN irldata.player_season_info psi ON psi.id = pp.player_season_id
                  WHERE pp.match_id = rm.id
              ),
              '[]'::json
          ) AS player_performances
      FROM relevant_matches rm
      JOIN irldata.tournament_info t ON t.id = rm.tournament_id
      JOIN irldata.team ht ON ht.id = rm.home_team_id
      JOIN irldata.team at ON at.id = rm.away_team_id
      ORDER BY rm.match_date ASC;
    `;

    const result = await client.query(sql, [userId]);

    return res.status(200).json(result.rows);

  } catch (err) {
    console.error("GET /matches/active failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET SPECIFIC MATCH DETAILS
   GET /matches/:matchId
   ======================================================================================= */
app.get("/matches/:matchId", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];
  const { matchId } = req.params;

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  if (!matchId) {
    return res.status(400).json({ message: "Match ID is required" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const sql = `
      SELECT
          m.id,
          m.match_date,
          m.tournament_id,
          m.season_id,
          m.venue_id,
          m.home_team_id,
          m.away_team_id,
          m.home_team_score,
          m.away_team_score,
          m.home_team_wickets,
          m.away_team_wickets,
          m.home_team_balls,
          m.away_team_balls,
          m.dls,
          m.inserted_at,
          m.status,
          m.home_match_num,
          m.away_match_num,
          ht.name AS home_team_name,
          ht.image AS home_team_image,
          ht.abbreviation AS home_team_abbreviation,
          at.name AS away_team_name,
          at.image AS away_team_image,
          at.abbreviation AS away_team_abbreviation
      FROM irldata.match_info m
      JOIN irldata.team ht ON ht.id = m.home_team_id
      JOIN irldata.team at ON at.id = m.away_team_id
      WHERE m.id = $1;
    `;

    const result = await client.query(sql, [matchId]);

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "Match not found" });
    }

    return res.status(200).json(result.rows[0]);

  } catch (err) {
    console.error("GET /matches/:matchId failed:", err);
    return res.status(500).json({ message: "Unexpected error occurred" });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
