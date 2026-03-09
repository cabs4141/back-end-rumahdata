import { pool } from "../../config/db.js";

export const getAllUserTeam = async (req, res) => {
    try {
        const query = `SELECT * FROM public.user_team ORDER BY id ASC`;
        const result = await pool.query(query);

        res.json({
            data: result.rows,
        });
    } catch (err) {
        console.error("GET USER TEAM ERROR:", err);
        res.status(500).json({ message: "Gagal mengambil data tim pengurus" });
    }
};

export const insertUserTeam = async (req, res) => {
    try {
        const { nama_tim } = req.body;
        if (!nama_tim) {
            return res.status(400).json({ message: "Nama tim wajib diisi" });
        }

        const result = await pool.query(
            `INSERT INTO public.user_team (nama_tim) VALUES ($1) RETURNING *`,
            [nama_tim]
        );

        res.status(201).json({ message: "Tim berhasil ditambahkan", data: result.rows[0] });
    } catch (err) {
        console.error("INSERT USER TEAM ERROR:", err);
        if (err.code === "23505") {
            return res.status(400).json({ message: "Nama tim sudah ada" });
        }
        res.status(500).json({ message: "Gagal menambahkan tim" });
    }
};

export const deleteUserTeam = async (req, res) => {
    try {
        const { id } = req.params;
        const result = await pool.query("DELETE FROM public.user_team WHERE id = $1 RETURNING *", [id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ message: "Tim tidak ditemukan" });
        }

        res.json({ message: "Tim berhasil dihapus" });
    } catch (err) {
        console.error("DELETE USER TEAM ERROR:", err);
        if (err.code === "23503") {
            return res.status(400).json({ message: "Gagal menghapus tim karena sedang digunakan pada kegiatan" });
        }
        res.status(500).json({ message: "Gagal menghapus tim" });
    }
};

export const updateUserTeam = async (req, res) => {
    try {
        const { id } = req.params;
        const { nama_tim } = req.body;

        if (!nama_tim) {
            return res.status(400).json({ message: "Nama tim wajib diisi" });
        }

        const result = await pool.query(
            "UPDATE public.user_team SET nama_tim = $1 WHERE id = $2 RETURNING *",
            [nama_tim, id]
        );

        if (result.rows.length === 0) {
            return res.status(404).json({ message: "Tim tidak ditemukan" });
        }

        res.json({ message: "Nama tim berhasil diperbarui", data: result.rows[0] });
    } catch (err) {
        console.error("UPDATE USER TEAM ERROR:", err);
        if (err.code === "23505") {
            return res.status(400).json({ message: "Nama tim sudah ada" });
        }
        res.status(500).json({ message: "Gagal memperbarui tim" });
    }
};
