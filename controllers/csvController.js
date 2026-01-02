import { pool } from "../src/db.js";
import jwt from "jsonwebtoken";

const getPTK = async (req, res) => {
  //validasi token udah di middleware
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = 10;
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

const getSekolah = async (req, res) => {
  // 🔐 VALIDASI TOKEN
  const authHeader = req.headers.authorization;
  if (!authHeader) {
    return res.status(401).json({ message: "Token tidak ditemukan" });
  }

  const token = authHeader.split(" ")[1];
  try {
    jwt.verify(token, process.env.JWT_SECRET);
  } catch (err) {
    return res.status(403).json({ message: "Token tidak valid" });
  }

  try {
    const page = parseInt(req.query.page) || 1;
    const limit = 10;
    const offset = (page - 1) * limit;

    const dataQuery = `
      SELECT *
      FROM public.data_sekolah
      ORDER BY sekolah_id
      LIMIT $1 OFFSET $2
    `;

    const countQuery = `
      SELECT COUNT(*) FROM public.data_sekolah
    `;

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
    console.error("SEKOLAH ERROR:", err);
    res.status(500).json({ message: "Gagal memproses data sekolah" });
  }
};

export { getPTK, getSekolah };
