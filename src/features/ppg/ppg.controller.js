import { pool } from "../../config/db.js";

export const getAllPPG = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const offset = (page - 1) * limit;

        const dataQuery = `
      SELECT *
      FROM public.ppg
      ORDER BY no::INTEGER ASC
      LIMIT $1 OFFSET $2
    `;

        const countQuery = `SELECT COUNT(*) FROM public.ppg`;

        const [dataResult, countResult] = await Promise.all([pool.query(dataQuery, [limit, offset]), pool.query(countQuery)]);

        const totalData = parseInt(countResult.rows[0].count);
        const totalPages = Math.ceil(totalData / limit);

        const cleanData = dataResult.rows.map((row) => {
            const obj = {};
            for (const key in row) {
                obj[key] = row[key] ?? "";
            }
            return obj;
        });

        res.json({
            page,
            limit,
            totalData,
            totalPages,
            data: cleanData,
        });
    } catch (err) {
        console.error("PPG ERROR:", err);
        res.status(500).json({ message: "Gagal memproses data PPG" });
    }
};

export const searchPPG = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const search = req.query.keyword || req.query.query || "";
        const offset = (page - 1) * limit;

        const dataQuery = `
      SELECT *
      FROM public.ppg
      WHERE nama_lengkap ILIKE $1
      ORDER BY nama_lengkap ASC
      LIMIT $2 OFFSET $3
    `;

        const countQuery = `
      SELECT COUNT(*) FROM public.ppg
      WHERE nama_lengkap ILIKE $1
    `;

        const searchParam = `%${search.trim()}%`;

        const [dataResult, countResult] = await Promise.all([pool.query(dataQuery, [searchParam, limit, offset]), pool.query(countQuery, [searchParam])]);

        const totalData = parseInt(countResult.rows[0].count);
        const totalPages = Math.ceil(totalData / limit);

        res.json({
            page,
            limit,
            totalData,
            totalPages,
            data: dataResult.rows,
        });
    } catch (err) {
        console.error("SEARCH PPG ERROR:", err);
        res.status(500).json({ message: "Gagal mencari data PPG" });
    }
};

export const deleteAllPPG = async (req, res) => {
    try {
        const checkData = await pool.query("SELECT COUNT(*) FROM public.ppg");
        const count = parseInt(checkData.rows[0].count);

        if (count === 0) {
            return res.status(400).json({
                message: "Data sudah kosong",
            });
        }

        await pool.query("TRUNCATE TABLE public.ppg RESTART IDENTITY CASCADE");

        res.json({
            message: `Data berhasil dihapus`,
        });
    } catch (err) {
        console.error("DELETE ALL PPG ERROR:", err);
        res.status(500).json({ message: "Gagal menghapus data" });
    }
};
