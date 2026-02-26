import { pool } from "../src/db.js";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

const register = async (req, res) => {
  try {
    const { nip, nama, password, id_bidang } = req.body;

    // Validasi wajib diisi
    if (!nip || !nama || !password) {
      return res
        .status(400)
        .json({ error: "NIP, nama, dan password wajib diisi" });
    }

    // Hash password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Insert ke database → sementara role = 'user'
    const result = await pool.query(
      `INSERT INTO users (nip, nama, password, role, id_bidang, status)
      VALUES ($1, $2, $3, 'user', $4, 'pending')
      RETURNING id, nip, nama, role, id_bidang, status`,
      [nip, nama, hashedPassword, id_bidang],
    );

    res.status(200).json({
      message: "Registrasi berhasil. Menunggu approval super admin",
      user: result.rows[0],
    });
  } catch (error) {
    console.error(error);

    // NIP sudah terdaftar
    if (error.code === "23505") {
      return res.status(400).json({ error: "NIP sudah terdaftar" });
    }

    res.status(500).json({ error: "Terjadi kesalahan server" });
  }
};

const login = async (req, res) => {
  try {
    const { nip, password } = req.body;
    const result = await pool.query("SELECT * FROM users WHERE nip = $1", [
      nip,
    ]);

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
