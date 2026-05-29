import { getPool } from "/opt/nodejs/index";
import type { SQSEvent } from 'aws-lambda';

interface MatchEndedMessage {
  matchId: string;
  seasonId: string;
  tournamentId: string;
}

export const lambdaHandler = async (event: SQSEvent): Promise<void> => {
  const pool = getPool();
  const client = await pool.connect();

  try {
    for (const record of event.Records) {
      const message: MatchEndedMessage = JSON.parse(record.body);
      const { matchId, seasonId, tournamentId } = message;

      console.log('Resolving matchups for finished match:', matchId);

      // 1. Find candidate matchups: any unresolved fantasy matchup in a league
      //    on this season+tournament whose match_num corresponds to the finished
      //    IRL match's home or away match_num.
      //    (No league.status filter -- if a matchup row exists, resolve it.)
      // 2. Determine which candidates are fully ready: every roster IRL team
      //    that has a match at the relevant match_num must be terminal
      //    (FINISHED or ABAN.).
      // 3. Score them via fantasydata.compute_matchup_scores.
      // 4. Persist the winner. (Scores are recomputed on demand by callers.)
      const sql = `
        WITH
        finished_match AS (
            SELECT id, home_match_num, away_match_num
            FROM irldata.match_info
            WHERE id = $1
        ),

        candidate_matchups AS (
            SELECT DISTINCT
                fm.id AS matchup_id,
                fm.match_num,
                fm.fantasy_team_instance1_id,
                fm.fantasy_team_instance2_id,
                l.season_id,
                l.tournament_id
            FROM fantasydata.fantasy_matchups fm
            JOIN fantasydata.leagues l ON l.id = fm.league_id
            CROSS JOIN finished_match fmi
            WHERE fm.fantasy_winner_team_instance_id IS NULL
              AND l.season_id = $2
              AND l.tournament_id = $3
              AND (fm.match_num = fmi.home_match_num OR fm.match_num = fmi.away_match_num)
        ),

        matchup_irl_teams AS (
            SELECT DISTINCT
                cm.matchup_id, cm.match_num,
                cm.season_id, cm.tournament_id,
                psi.team_id
            FROM candidate_matchups cm
            JOIN fantasydata.fantasy_team_instance ti
                ON ti.id IN (cm.fantasy_team_instance1_id, cm.fantasy_team_instance2_id)
            CROSS JOIN LATERAL (VALUES
                (ti.bat1),(ti.bat2),(ti.wicket1),
                (ti.bowl1),(ti.bowl2),(ti.bowl3),
                (ti.all1),(ti.flex1)
            ) AS u(player_id)
            JOIN irldata.player_season_info psi
                ON psi.player_id = u.player_id
               AND psi.season_id = cm.season_id
               AND psi.tournament_id = cm.tournament_id
            WHERE u.player_id IS NOT NULL
        ),

        fully_finished_matchups AS (
            SELECT mt.matchup_id
            FROM matchup_irl_teams mt
            JOIN irldata.match_info mi
                ON mi.season_id = mt.season_id
               AND mi.tournament_id = mt.tournament_id
               AND (
                    (mi.home_team_id = mt.team_id AND mi.home_match_num = mt.match_num)
                    OR (mi.away_team_id = mt.team_id AND mi.away_match_num = mt.match_num)
               )
            GROUP BY mt.matchup_id
            HAVING COUNT(*) > 0
               AND COUNT(*) FILTER (WHERE mi.status NOT IN ('FINISHED', 'ABAN.')) = 0
        ),

        scored AS (
            SELECT * FROM fantasydata.compute_matchup_scores(
                ARRAY(SELECT matchup_id FROM fully_finished_matchups)
            )
        )

        UPDATE fantasydata.fantasy_matchups fm
        SET fantasy_winner_team_instance_id = CASE
                WHEN s.team1_score > s.team2_score THEN fm.fantasy_team_instance1_id
                WHEN s.team2_score > s.team1_score THEN fm.fantasy_team_instance2_id
                ELSE fm.fantasy_team_instance1_id  -- tie-breaker: team 1 (home) wins
            END
        FROM scored s
        WHERE fm.id = s.matchup_id;
      `;

      const result = await client.query(sql, [matchId, seasonId, tournamentId]);
      console.log(`Resolved ${result.rowCount} matchup(s) for match ${matchId}`);
    }
  } catch (err) {
    console.error('Matchup resolution failed:', err);
    throw err; // Let SQS retry
  } finally {
    client.release();
  }
};
