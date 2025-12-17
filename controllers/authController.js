import { pool } from "../src/db.js";
import bcrypt from "bcryptjs";
import jwt from "jsonwebtoken";

// REGISTER = pending
export const register = async (req, res) => {
  try {
    const { nip, nama, password, id_bidang } = req.body;

    // Validasi wajib diisi
    if (!nip || !nama || !password) {
      return res.status(400).json({ error: "NIP, nama, dan password wajib diisi" });
    }

    // Hash password
    const hashedPassword = await bcrypt.hash(password, 10);

    // Insert ke database → sementara role = 'user'
    const result = await pool.query(
      `INSERT INTO users (nip, nama, password, role, id_bidang, status)
      VALUES ($1, $2, $3, 'user', $4, 'pending')
      RETURNING id, nip, nama, role, id_bidang, status`,
      [nip, nama, hashedPassword, id_bidang]
    );

    res.json({
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

// LOGIN = approve
export const login = async (req, res) => {
  try {
    const { nip, password } = req.body;

    const result = await pool.query("SELECT * FROM users WHERE nip = $1", [nip]);

    if (result.rows.length === 0) {
      return res.status(400).json({ error: "NIP tidak ditemukan" });
    }

    const user = result.rows[0];

    // Cek status
    if (user.status !== 'approved') {
      return res.status(403).json({ error: "Akun Anda belum diaktifkan, hubungi super admin" });
    }

    // Cek password
    const validPass = await bcrypt.compare(password, user.password);
    if (!validPass) {
      return res.status(400).json({ error: "Password salah" });
    }

    // Ambil role dari database
    const userRole = user.role;

    // Buat token JWT
    const token = jwt.sign(
      { id: user.id, nip: user.nip, role: userRole, id_bidang: user.id_bidang },
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
        id_bidang: user.id_bidang,
      },
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Terjadi kesalahan server" });
  }
};

// LOGOUT
export const logout = (req, res) => {
  res.json({ message: "Logout berhasil" });
};