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
                        'away_team_name', at.name,
                        'away_team_image', at.image
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
          at.name AS away_team_name,
          at.image AS away_team_image
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
