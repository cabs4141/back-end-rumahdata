import { pool } from "../../config/db.js";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const register = async (req, res) => {
  try {
    const { nip, nama, password, role } = req.body;

    // 1. Validasi input wajib
    if (!nip || !nama || !password) {
      return res.status(400).json({ error: "NIP, nama, dan password wajib diisi" });
    }

    // 2. Hash password sebelum masuk ke database
    const saltRounds = 10;
    const hashedPassword = await bcrypt.hash(password, saltRounds);

    // 3. Simpan ke database
    // Kolom 'role' akan otomatis menjadi 'user' jika tidak dikirim (karena default 'user'::user_role)
    // created_at dan updated_at juga otomatis terisi oleh CURRENT_TIMESTAMP
    const query = `
      INSERT INTO users (nip, nama, password, role)
      VALUES ($1, $2, $3, $4)
      RETURNING id, nip, nama, role, created_at
    `;

    // Jika role tidak dikirim di body, kita kirimkan undefined agar DB menggunakan DEFAULT value-nya
    const values = [nip, nama, hashedPassword, role || "user"];

    const result = await pool.query(query, values);

    res.status(201).json({
      message: "User berhasil didaftarkan",
      user: result.rows[0],
    });
  } catch (error) {
    console.error("Error Register:", error.message);

    // Cek jika NIP duplikat (asumsi ada UNIQUE constraint pada NIP)
    if (error.code === "23505") {
      return res.status(400).json({ error: "NIP sudah terdaftar" });
    }

    // Cek jika role yang dikirim tidak sesuai dengan enum 'user_role'
    if (error.code === "22P02") {
      return res.status(400).json({ error: "Role tidak valid" });
    }

    res.status(500).json({ error: "Terjadi kesalahan server" });
  }
};

const login = async (req, res) => {
  try {
    const { nip, password } = req.body;
    const result = await pool.query("SELECT * FROM users WHERE nip = $1", [nip]);

    if (result.rows.length === 0) {
      return res.status(400).json({ message: "NIP tidak ditemukan" });
    }

    const user = result.rows[0];

    // Cek password
    const validPass = await bcrypt.compare(password, user.password);
    if (!validPass) {
      return res.status(400).json({ message: "Password salah" });
    }
    const permissionResult = await pool.query(
      `
      SELECT p.code
      FROM permissions p
      JOIN user_permissions up ON p.id = up.permission_id
      WHERE up.user_id = $1
      `,
      [user.id],
    );

    const permissions = permissionResult.rows.map((p) => p.code);

    // Ambil role dari database
    const userRole = user.role;
    const token = jwt.sign(
      {
        id: user.id,
        nip: user.nip,
        nama: user.nama,
        role: userRole,
      },
      process.env.JWT_SECRET,
      { expiresIn: "3d" },
    );

    res.status(200).json({
      message: "Login berhasil",
      token,
      user: {
        id: user.id,
        nip: user.nip,
        nama: user.nama,
        role: user.role,
      },
      permissions,
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Terjadi kesalahan server" });
  }
};

const createUser = async (req, res) => {
  const { nip, nama, password, role, permissions } = req.body;

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const hashedPassword = await bcrypt.hash(password, 10);

    const userResult = await client.query(
      `
      INSERT INTO users (nip, nama, password, role)
      VALUES ($1, $2, $3, $4)
      RETURNING id
      `,
      [nip, nama, hashedPassword, role || "user"],
    );

    const userId = userResult.rows[0].id;

    let permissionResult = { rows: [] };
    if (Array.isArray(permissions) && permissions.length > 0) {
      permissionResult = await client.query(
        `
        SELECT id FROM permissions
        WHERE code = ANY($1::text[])
        `,
        [permissions],
      );
    }

    for (const row of permissionResult.rows) {
      await client.query(
        `
        INSERT INTO user_permissions (user_id, permission_id)
        VALUES ($1, $2)
        `,
        [userId, row.id],
      );
    }

    await client.query("COMMIT");

    res.status(201).json({
      message: "User berhasil dibuat",
      userId,
    });
  } catch (error) {
    await client.query("ROLLBACK");
    if (error && error.code === "23505") {
      return res.status(400).json({ message: "NIP sudah terdaftar" });
    }
    res.status(500).json({ message: "Gagal membuat user" });
  } finally {
    client.release();
  }
};

export { login, register, createUser };
