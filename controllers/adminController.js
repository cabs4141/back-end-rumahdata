import { pool } from "../src/db.js";

// Super Admin approve user
export const approveUser = async (req, res) => {
  try {
    const { id_user, role } = req.body;

    // Validasi role
    if (!['admin', 'user'].includes(role)) {
      return res.status(400).json({ error: "Role harus admin atau user" });
    }

    // Update status dan role user
    const result = await pool.query(
      `UPDATE users 
       SET role = $1, status = 'approved'
       WHERE id = $2
       RETURNING id, nip, nama, role, id_bidang, status`,
      [role, id_user]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "User tidak ditemukan" });
    }

    res.json({
      message: `User berhasil diapprove sebagai ${role}`,
      user: result.rows[0],
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "Terjadi kesalahan server" });
  }
};