import { pool } from "../src/db.js";

const getUsersList = async (req, res) => {
  try {
    const query = `
        SELECT 
            id,
            nip,
            nama,
            role,
            status
        FROM users
        ORDER BY id ASC
        `;

    const result = await pool.query(query);

    res.status(200).json({
      success: true,
      total: result.rowCount,
      data: result.rows,
    });
  } catch (error) {
    console.error("Get users error:", error);
    res.status(500).json({
      success: false,
      message: "Gagal mengambil data users",
    });
  }
};

const approveUser = async (req, res) => {
  try {
    const { id_user, role, status } = req.body;

    //Ambil data user dari database
    const currentUser = await pool.query("SELECT role, status FROM users WHERE id = $1", [id_user]);

    if (currentUser.rows.length === 0) {
      return res.status(404).json({ error: "User tidak ditemukan" });
    }

    //nilai dari body, kalau kosong gunakan nilai lama
    let newRole = role || currentUser.rows[0].role;
    let newStatus = status || currentUser.rows[0].status;

    //Validasi sebelum masuk ke DB
    if (newRole && !["admin", "user"].includes(newRole)) {
      return res.status(400).json({ error: "Role tidak valid" });
    }
    if (newStatus && !["pending", "approved"].includes(newStatus)) {
      return res.status(400).json({ error: "Status tidak valid" });
    }

    //Update
    const result = await pool.query(
      `UPDATE users 
       SET role = $1, status = $2
       WHERE id = $3
       RETURNING id, nip, nama, role, status`,
      [newRole, newStatus, id_user]
    );

    res.json({
      message: "Data user berhasil diperbarui",
      user: result.rows[0],
      autoApproved: role === "admin", // Memberitahu frontend jika terjadi auto-approve
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: "server error" });
  }
};

const getUserById = async (req, res) => {
  try {
    const { id } = req.params;
    const query = `
      SELECT id, nama, nip, role, status, created_at 
      FROM public.users 
      WHERE id = $1
    `;

    const result = await pool.query(query, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({
        message: `User dengan ID ${id} tidak ditemukan`,
      });
    }

    // Mengembalikan data user (baris pertama)
    res.json({
      message: "success",
      data: result.rows[0],
    });
  } catch (err) {
    console.error("GET USER BY ID ERROR:", err);
    res.status(500).json({
      message: "Terjadi kesalahan pada server",
      error: err.message,
    });
  }
};

const deleteUserById = async (req, res) => {
  try {
    const { id } = req.params; // Mengambil ID dari URL parameter

    // 1. Cek apakah user ada di database
    const checkQuery = `SELECT id, nama FROM public.users WHERE id = $1`;
    const checkRes = await pool.query(checkQuery, [id]);

    if (checkRes.rows.length === 0) {
      return res.status(404).json({
        message: "User tidak ditemukan",
      });
    }

    const userName = checkRes.rows[0].nama;

    // 2. Jalankan perintah hapus
    const deleteQuery = `DELETE FROM public.users WHERE id = $1`;
    await pool.query(deleteQuery, [id]);

    // 3. Response sukses
    res.status(200).json({
      message: `User ${userName} berhasil dihapus`,
      deletedId: id,
    });
  } catch (err) {
    console.error("DELETE USER ERROR:", err);
    res.status(500).json({
      message: "Terjadi kesalahan pada server saat menghapus user",
    });
  }
};

export { getUsersList, approveUser, getUserById, deleteUserById };
