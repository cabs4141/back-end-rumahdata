import { pool } from "../../config/db.js";

    /* ======================
    GET DESKRIPSI
    ====================== */
    const getDeskripsi = async (req, res) => {
    try {
        const result = await pool.query(
        `SELECT isi FROM deskripsi LIMIT 1`
        );

        res.json({
        isi: result.rows[0]?.isi ?? "",
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Gagal mengambil deskripsi" });
    }
    };

    /* ======================
    UPDATE DESKRIPSI
    ====================== */
    const updateDeskripsi = async (req, res) => {
    try {
        const { isi } = req.body;

        await pool.query(
        `UPDATE deskripsi SET isi = $1`,
        [isi]
        );

        res.json({
        message: "Deskripsi berhasil diperbarui",
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({ message: "Gagal update deskripsi" });
    }
    };

    export { getDeskripsi, updateDeskripsi };
