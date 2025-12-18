import { pool } from "../src/db.js";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

// REGISTER = pending
export const register = async (req, res) => {
  try {
    const { nip, nama, password } = req.body;

    if (!nip || !nama || !password) {
      return res.status(400).json({
        error: "NIP, nama, dan password wajib diisi",
      });
    }

    // Hash password
    const hashedPassword = await bcrypt.hash(password, 10);

    const result = await pool.query(
      `INSERT INTO users (nip, nama, password)
       VALUES ($1, $2, $3)
       RETURNING id, nip, nama`,
      [nip, nama, hashedPassword]
    );

    res.json({
      message: "Registrasi berhasil, menunggu persetujuan superadmin",
      user: result.rows[0],
    });
  } catch (error) {
    if (error.code === "23505") {
      return res.status(400).json({ error: "NIP sudah terdaftar" });
    }
    res.status(500).json({ error: "Server error" });
  }
};


// LOGIN dengan JWT
export const login = async (req, res) => {
  try {
    const { nip, password } = req.body;

    const result = await pool.query(
      "SELECT * FROM users WHERE nip = $1",
      [nip]
    );

    if (result.rows.length === 0) {
      return res.status(400).json({ error: "NIP tidak ditemukan" });
    }

    const user = result.rows[0];

    if (!user.is_approved) {
      return res.status(403).json({
        error: "Akun belum disetujui superadmin",
      });
    }

    const validPass = await bcrypt.compare(password, user.password);
    if (!validPass) {
      return res.status(400).json({ error: "Password salah" });
    }

    const token = jwt.sign(
      {
        id: user.id,
        nip: user.nip,
        role: user.role, // role hanya dari DB
      },
      process.env.JWT_SECRET,
      { expiresIn: "1h" }
    );

    res.json({
      message: "Login berhasil",
      token,
      user: {
        id: user.id,
        nip: user.nip,
        nama: user.nama,
        role: user.role,
      },
    });
  } catch {
    res.status(500).json({ error: "Server error" });
  }
};

export const approveUser = async (req, res) => {
  try {
    const { userId, role } = req.body;

    if (!["user", "admin"].includes(role)) {
      return res.status(400).json({
        error: "Role tidak valid",
      });
    }

    const result = await pool.query(
      `UPDATE users
       SET is_approved = true, role = $1
       WHERE id = $2
       RETURNING id, nip, nama, role, is_approved`,
      [role, userId]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "User tidak ditemukan" });
    }

    res.json({
      message: "User berhasil disetujui",
      user: result.rows[0],
    });
  } catch {
    res.status(500).json({ error: "Server error" });
  }
};


export const logout = (req, res) => {
  res.json({ message: "Logout berhasil" });
};