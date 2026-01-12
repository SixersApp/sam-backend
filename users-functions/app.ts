import { getPool, createApp, createHandler, Request, Response } from "/opt/nodejs/index";

const app = createApp();

/* =======================================================================================
   CREATE OR UPDATE USER AUTH AND DEFAULT PROFILE DATA
   ======================================================================================= */
app.put("/users/auth/signup", async (req: Request, res: Response) => {
  if (!req.body) {
    return res.status(400).json({ message: "Missing request body" });
  }

  const userData = req.body;

  const username = req.lambdaEvent.requestContext.authorizer?.claims?.["cognito:sub"];

  if (!username) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    const result = await client.query(
      `
      SELECT * FROM authdata.app_user WHERE id = $1
      `,
      [username]
    );

      if (result.rowCount && result.rowCount > 0) {
        return res.status(200).json({ message: "User already exists", userId: username });
      }


    await client.query('BEGIN');

    // ---------- UPSERT app_user ----------
    await client.query(
      `
      INSERT INTO authdata.app_user (id, email, created_at)
      VALUES ($1, $2, NOW())
      ON CONFLICT (id)
      DO UPDATE SET
        email = EXCLUDED.email;
      `,
      [username, userData.email]
    );

    // ---------- UPSERT profile ----------
    await client.query(
      `
      INSERT INTO authdata.profiles
      (user_id, full_name, avatar_url, dob, country, experience, onboarding_stage, created_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
      ON CONFLICT (user_id)
      DO UPDATE SET
        full_name = EXCLUDED.full_name,
        avatar_url = EXCLUDED.avatar_url,
        dob = EXCLUDED.dob,
        country = EXCLUDED.country,
        experience = EXCLUDED.experience,
        onboarding_stage = EXCLUDED.onboarding_stage;
      `,
      [
        username,
        userData.fullName ?? null,
        userData.avatar_url ?? null,
        userData.dob ?? null,
        userData.country ?? null,
        userData.experience ?? null,
        userData.onboarding_stage ?? 0
      ]
    );

    await client.query('COMMIT');

    return res.status(200).json({ message: "User profile created/updated successfully", userId: username });
  } catch (err) {
    console.error('PUT /profile failed:', err);
    await client?.query('ROLLBACK');

    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

/* =======================================================================================
   GET USER PROFILE DATA
   ======================================================================================= */
app.get("/users/profile", async (req: Request, res: Response) => {
  const userId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  console.log("\n\n\n\n", userId, "\n\n\n\n");

  if (!userId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  let client;

  try {
      const pool = getPool();
      client = await pool.connect();

      const result = await client.query(
          `SELECT
              user_id,
              full_name,
              country,
              dob,
              onboarding_stage,
              experience,
              created_at
            FROM authdata.profiles
            WHERE user_id = $1`,
          [userId]
      );

      if (result.rowCount === 0) {
          return res.status(404).json({ message: "Profile not found" });
      }

      return res.status(200).json({ message: "Profile retrieved successfully", profile: result.rows[0] });

  } catch (err: any) {
      console.error(err);
      return res.status(500).json({
          message: "Internal server error",
          error: err.message
      });
  } finally {
      if (client) client.release();
  }
});

/* =======================================================================================
   PATCH USER PROFILE (PARTIAL UPDATE)
   ======================================================================================= */
app.patch("/users/profile", async (req: Request, res: Response) => {
  const tokenUserId =
    req.lambdaEvent.requestContext.authorizer?.claims?.["sub"];

  if (!tokenUserId) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  const body = req.body ?? {};

  let {
    full_name,
    country,
    dob,
    onboarding_stage,
    experience
  } = body;

  if (
    full_name === undefined &&
    country === undefined &&
    dob === undefined &&
    onboarding_stage === undefined &&
    experience === undefined
  ) {
    return res.status(400).json({
      message: "No valid onboarding fields provided"
    });
  }

  if (full_name !== undefined && typeof full_name !== "string") {
    return res.status(400).json({ message: "full_name must be a string" });
  }

  if (country !== undefined && typeof country !== "string") {
    return res.status(400).json({ message: "country must be a string" });
  }

  if (dob !== undefined) {
    if (typeof dob !== "string" || isNaN(Date.parse(dob))) {
      return res.status(400).json({
        message: "dob must be a valid date string (YYYY-MM-DD)"
      });
    }
    dob = new Date(dob).toISOString().split("T")[0];
  }

  if (onboarding_stage !== undefined) {
    if (typeof onboarding_stage !== "number") {
      return res.status(400).json({
        message: "onboarding_stage must be an integer"
      });
    }
    onboarding_stage = Math.floor(onboarding_stage);
  }

  if (experience !== undefined) {
    if (typeof experience !== "number") {
      return res.status(400).json({
        message: "experience must be an integer"
      });
    }
    experience = Math.floor(experience);
  }

  const updates: string[] = [];
  const columns = ["user_id"];
  const placeholders = ["$1"];
  const values: any[] = [tokenUserId];

  let index = 2;

  function addField(column: string, value: any) {
    if (value === undefined) return;
    columns.push(column);
    placeholders.push(`$${index}`);
    updates.push(`${column} = EXCLUDED.${column}`);
    values.push(value);
    index++;
  }

  addField("full_name", full_name);
  addField("country", country);
  addField("dob", dob);
  addField("onboarding_stage", onboarding_stage);
  addField("experience", experience);

  const sql = `
    INSERT INTO authdata.profiles (${columns.join(", ")})
    VALUES (${placeholders.join(", ")})
    ON CONFLICT (user_id)
    DO UPDATE SET ${updates.join(", ")};
  `;

  let client;

  try {
    const pool = getPool();
    client = await pool.connect();

    await client.query(sql, values);

    return res.status(200).json({
      message: "Profile updated successfully",
      userId: tokenUserId,
      updated_fields: columns.slice(1)
    });

  } catch (err: any) {
    console.error("PATCH /users/profile failed:", err);
    return res.status(500).json({ message: "Internal server error" });
  } finally {
    client?.release();
  }
});

export const lambdaHandler = createHandler(app);
