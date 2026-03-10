import { pool } from "../../config/db.js";

// 1. GET ALL USERS
const getUsersList = async (req, res) => {
  try {
    // Menghapus 'status', menambahkan 'created_at' dan 'updated_at'
    const query = `
        SELECT 
            id,
            nip,
            nama,
            role,
            created_at,
            updated_at
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

// 2. UPDATE USER (Pengganti approveUser)
// Digunakan untuk mengupdate role dan/atau permissions user
const updateUser = async (req, res) => {
  // Menggunakan transaction (client) karena kita mungkin update tabel users dan user_permissions sekaligus
  const client = await pool.connect();

  try {
    const { id } = req.params; // Mengambil ID dari URL parameter
    const { role, permissions } = req.body; // permissions berupa array ID, contoh: [1, 2, 3]

    // Cek apakah user eksis
    const userCheck = await client.query("SELECT id FROM users WHERE id = $1", [id]);
    if (userCheck.rows.length === 0) {
      return res.status(404).json({ error: "User tidak ditemukan" });
    }

    await client.query("BEGIN"); // Mulai transaksi database

    // A. Update Role (jika dikirim di body)
    if (role) {
      await client.query(`UPDATE users SET role = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, [role, id]);
    }

    // B. Update Permissions (jika dikirim di body)
    if (permissions && Array.isArray(permissions)) {
      // Hapus permission lama
      await client.query(`DELETE FROM user_permissions WHERE user_id = $1`, [id]);

      // Insert permission baru jika array tidak kosong
      if (permissions.length > 0) {
        // Membuat string query dinamis, misal: ($1, $2), ($1, $3)
        const insertValues = permissions.map((permissionId) => `(${id}, ${permissionId})`).join(", ");
        await client.query(`INSERT INTO user_permissions (user_id, permission_id) VALUES ${insertValues}`);
      }
    }

    await client.query("COMMIT"); // Simpan perubahan

    res.json({
      success: true,
      message: "Data user dan hak akses berhasil diperbarui",
    });
  } catch (error) {
    await client.query("ROLLBACK"); // Batalkan jika ada error
    console.error("UPDATE USER ERROR:", error);

    // Tangani error enum role tidak valid
    if (error.code === "22P02") {
      return res.status(400).json({ error: "Role yang dimasukkan tidak valid" });
    }

    res.status(500).json({ error: "Terjadi kesalahan server saat update user" });
  } finally {
    client.release(); // Kembalikan koneksi ke pool
  }
};

// 3. GET USER BY ID
const getUserById = async (req, res) => {
  try {
    const { id } = req.params;

    // Menggunakan LEFT JOIN untuk langsung menarik daftar permissions (kode) yang dimiliki user
    const query = `
      SELECT 
        u.id, u.nama, u.nip, u.role, u.created_at, u.updated_at,
        COALESCE(json_agg(p.code) FILTER (WHERE p.code IS NOT NULL), '[]') as permissions
      FROM public.users u
      LEFT JOIN public.user_permissions up ON u.id = up.user_id
      LEFT JOIN public.permissions p ON up.permission_id = p.id
      WHERE u.id = $1
      GROUP BY u.id
    `;

    const result = await pool.query(query, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: `User dengan ID ${id} tidak ditemukan`,
      });
    }

    res.json({
      success: true,
      message: "success",
      data: result.rows[0],
    });
  } catch (err) {
    console.error("GET USER BY ID ERROR:", err);
    res.status(500).json({
      success: false,
      message: "Terjadi kesalahan pada server",
      error: err.message,
    });
  }
};

// 4. DELETE USER
const deleteUserById = async (req, res) => {
  try {
    const { id } = req.params;

    const checkQuery = `SELECT id, nama FROM public.users WHERE id = $1`;
    const checkRes = await pool.query(checkQuery, [id]);

    if (checkRes.rows.length === 0) {
      return res.status(404).json({
        success: false,
        message: "User tidak ditemukan",
      });
    }

    const userName = checkRes.rows[0].nama;

    // Menghapus user. Berkat ON DELETE CASCADE di database,
    // data di tabel user_permissions dan kegiatan akan otomatis terhapus.
    const deleteQuery = `DELETE FROM public.users WHERE id = $1`;
    await pool.query(deleteQuery, [id]);

    res.status(200).json({
      success: true,
      message: `User ${userName} berhasil dihapus`,
      deletedId: id,
    });
  } catch (err) {
    console.error("DELETE USER ERROR:", err);
    res.status(500).json({
      success: false,
      message: "Terjadi kesalahan pada server saat menghapus user",
    });
  }
};

export { getUsersList, updateUser, getUserById, deleteUserById };
