import { pool } from "../src/db.js";

const getPTK = async (req, res) => {
  //validasi token udah di middleware
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

const getSekolah = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
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

const deleteAllPtk = async (req, res) => {
  try {
    // Menggunakan TRUNCATE lebih cepat untuk menghapus semua data
    await pool.query("TRUNCATE TABLE public.ptk RESTART IDENTITY CASCADE");

    res.json({
      message: "Seluruh data PTK telah berhasil dihapus dan ID telah di-reset",
    });
  } catch (err) {
    console.error("DELETE ALL PTK ERROR:", err);
    res.status(500).json({ message: "Gagal menghapus semua data PTK" });
  }
};

const deleteAllSekolah = async (req, res) => {
  try {
    await pool.query("TRUNCATE TABLE public.data_sekolah RESTART IDENTITY CASCADE");

    res.json({
      message: "Seluruh data sekolah telah berhasil dihapus dan ID telah di-reset",
    });
  } catch (err) {
    console.error("DELETE ALL SEKOLAH ERROR:", err);
    res.status(500).json({ message: "Gagal menghapus semua data sekolah" });
  }
};

const addKegiatan = async (req, res) => {
  const client = await pool.connect();

  try {
    const { nama_kegiatan, tanggal_pelaksanaan, jumlah_peserta, pic, kelengkapan } = req.body;

    // 🔐 dari token
    const user_id = req.user.id;
    const id_bidang = req.user.id_bidang;

    await client.query("BEGIN");

    const kegiatanResult = await client.query(
      `
      INSERT INTO kegiatan (
        nama_kegiatan,
        tanggal_pelaksanaan,
        jumlah_peserta,
        pic,
        user_id,
        id_bidang
      )
      VALUES ($1, $2, $3, $4, $5, $6)
      RETURNING id
      `,
      [nama_kegiatan, tanggal_pelaksanaan, jumlah_peserta, pic, user_id, id_bidang],
    );

    const kegiatanId = kegiatanResult.rows[0].id;

    await client.query(
      `
      INSERT INTO kelengkapan_kegiatan (
        kegiatan_id,
        foto,
        video,
        upload_laporan
      )
      VALUES ($1, $2, $3, $4)
      `,
      [kegiatanId, kelengkapan?.foto ?? false, kelengkapan?.video ?? false, kelengkapan?.upload_laporan ?? false],
    );

    await client.query("COMMIT");

    res.status(201).json({
      message: "Kegiatan berhasil ditambahkan",
    });
  } catch (error) {
    await client.query("ROLLBACK");
    console.error(error);
    res.status(500).json({ message: "Gagal menambahkan kegiatan" });
  } finally {
    client.release();
  }
};

const searchPTK = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const search = req.query.query || "";
    const offset = (page - 1) * limit;

    // Query untuk data: Cek ptk_id (exact) atau nama (fuzzy)
    const dataQuery = `
      SELECT * FROM public.ptk
      WHERE ptk_id ILIKE $1 
         OR nama ILIKE $2 
         OR nip ILIKE $2
      ORDER BY nama ASC
      LIMIT $3 OFFSET $4
    `;

    // 2. Update countQuery: Samakan logika WHERE-nya
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

const searchSekolah = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const search = req.query.query || "";
    const offset = (page - 1) * limit;

    const dataQuery = `
      SELECT * FROM public.data_sekolah
      WHERE sekolah_id ILIKE $1 OR nama ILIKE $2
      ORDER BY nama ASC
      LIMIT $3 OFFSET $4
    `;

    const countQuery = `
      SELECT COUNT(*) FROM public.data_sekolah
      WHERE sekolah_id ILIKE $1 OR nama ILIKE $2
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
    console.error("SEARCH SEKOLAH ERROR:", err);
    res.status(500).json({ message: "Gagal memproses data sekolah" });
  }
};

const getSekolahDetail = async (req, res) => {
  try {
    const { sekolah_id } = req.params;
    const search = req.query.query || "";

    // 1. Query Profil Sekolah
    const sekolahQuery = `
      SELECT nama, npsn, alamat_jalan, email 
      FROM public.data_sekolah 
      WHERE LOWER(sekolah_id) = LOWER($1)
    `;
    const sekolahRes = await pool.query(sekolahQuery, [sekolah_id]);

    if (sekolahRes.rows.length === 0) {
      return res.status(404).json({ message: "Sekolah tidak ditemukan" });
    }

    const { nama: namaSekolah, npsn: npsnSekolah, alamat_jalan, email } = sekolahRes.rows[0];

    // 2. Query Seluruh Data PTK tanpa LIMIT dan OFFSET
    const dataQuery = `
      SELECT ptk_id, nama, semester, nik, nip, jenis_ptk 
      FROM public.ptk
      WHERE LOWER(sekolah_id) = LOWER($1) 
      AND (nama ILIKE $2)
      ORDER BY nama ASC
    `;

    // Query count tetap diperlukan untuk informasi total data di UI
    const countQuery = `
      SELECT COUNT(*) FROM public.ptk
      WHERE LOWER(sekolah_id) = LOWER($1)
      AND (nama ILIKE $2)
    `;

    const searchParam = `%${search}%`;
    const values = [sekolah_id, searchParam];

    const [dataResult, countResult] = await Promise.all([
      pool.query(dataQuery, values), // Hapus limit & offset dari sini
      pool.query(countQuery, values),
    ]);

    const totalData = parseInt(countResult.rows[0].count);

    // Response JSON (Hanya mengirim data yang diperlukan)
    res.json({
      sekolah_terpilih: namaSekolah,
      alamat: alamat_jalan,
      email: email,
      npsn: npsnSekolah,
      sekolah_id: sekolah_id.toLowerCase(),
      totalData: totalData,
      data_ptk: dataResult.rows, // Berisi seluruh data PTK
    });
  } catch (err) {
    console.error("GET PTK BY SEKOLAH ERROR:", err);
    res.status(500).json({ message: "Gagal memuat data" });
  }
};

export { getPTK, getSekolah, deleteAllPtk, deleteAllSekolah, addKegiatan, searchPTK, searchSekolah, getSekolahDetail };
