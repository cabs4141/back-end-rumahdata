import { pool } from "../../config/db.js";

export const getPTK = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;

    const dataQuery = `
      SELECT *
      FROM public.ptk
      ORDER BY ptk_id
      LIMIT $1 OFFSET $2
    `;

    const countQuery = `SELECT COUNT(*) FROM public.ptk`;
    const [dataResult, countResult] = await Promise.all([pool.query(dataQuery, [limit, offset]), pool.query(countQuery)]);
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
    console.error("PTK ERROR:", err);
    res.status(500).json({ message: "Gagal memproses data PTK" });
  }
};

export const getPtkStatistik = async (req, res) => {
  try {
    // Only select the required columns that actually exist in the DB schema
    const dataQuery = `
      SELECT ptk_id, sekolah_id, kabupaten, jenis_kelamin, 
             status_kepegawaian, jenis_ptk, jabatan_ptk
      FROM public.ptk
    `;

    const [dataResult] = await Promise.all([pool.query(dataQuery)]);

    res.json({
      totalData: dataResult.rows.length,
      data: dataResult.rows,
    });
  } catch (err) {
    console.error("PTK STATISTIK ERROR:", err);
    res.status(500).json({ message: "Gagal memproses data statistik PTK" });
  }
};

export const searchPTK = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const search = req.query.query || "";
    const offset = (page - 1) * limit;

    const dataQuery = `
      SELECT * FROM public.ptk
      WHERE ptk_id ILIKE $1 
         OR nama ILIKE $2 
         OR nip ILIKE $2
      ORDER BY nama ASC
      LIMIT $3 OFFSET $4
    `;

    const countQuery = `
      SELECT COUNT(*) FROM public.ptk
      WHERE LOWER(ptk_id) = LOWER($1) 
         OR nama ILIKE $2 
         OR nip ILIKE $2
    `;

    const searchParam = `%${search}%`;
    const values = [search, searchParam];

    const [dataResult, countResult] = await Promise.all([pool.query(dataQuery, [...values, limit, offset]), pool.query(countQuery, values)]);

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
    console.error("SEARCH PTK ERROR:", err);
    res.status(500).json({ message: "Gagal memproses data PTK" });
  }
};

export const deleteAllPtk = async (req, res) => {
  try {
    await pool.query("TRUNCATE TABLE public.ptk RESTART IDENTITY CASCADE");

    res.json({
      message: "Seluruh data PTK telah berhasil dihapus dan ID telah di-reset",
    });
  } catch (err) {
    console.error("DELETE ALL PTK ERROR:", err);
    res.status(500).json({ message: "Gagal menghapus semua data PTK" });
  }
};
