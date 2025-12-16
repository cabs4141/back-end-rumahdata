import { pool } from "../src/db.js";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

// REGISTER
export const register = async (req, res) => {
  try {
    const { nip, password } = req.body;

    const hashedPassword = await bcrypt.hash(password, 10);

    const result = await pool.query(
      "INSERT INTO users (nip, password) VALUES ($1, $2) RETURNING id, nip",
      [nip, hashedPassword]
    );

    res.json({
      message: "Registrasi berhasil",
      user: result.rows[0],
    });
  } catch (error) {
    console.error(error);

    if (error.code === "23505") {
      return res.status(400).json({ error: "NIP sudah terdaftar" });
    }

    res.status(500).json({ error: "Terjadi kesalahan server" });
  }
};

// LOGIN dengan JWT
export const login = async (req, res) => {
  try {
    const { nip, password } = req.body;

    const result = await pool.query("SELECT * FROM users WHERE nip = $1", [
      nip,
    ]);

    if (result.rows.length === 0) {
      return res.status(400).json({ error: "NIP tidak ditemukan" });
    }

    const user = result.rows[0];

    const validPass = await bcrypt.compare(password, user.password);
    if (!validPass) {
      return res.status(400).json({ error: "Password salah" });
    }

    const userLevel = nip === "123456" ? "admin" : "user"; // contoh sederhana
    // Buat token JWT
    const token = jwt.sign(
      { id: user.id, nip: user.nip, userLevel }, // userLevel hanya di token
      process.env.JWT_SECRET,
      { expiresIn: "1h" }
    );

    res.json({
      message: "Login berhasil",
      token,
      user: { id: user.id, nip: user.nip },
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Terjadi kesalahan server" });
  }
};

export const logout = (req, res) => {
  res.json({ message: "Logout berhasil" });
};

