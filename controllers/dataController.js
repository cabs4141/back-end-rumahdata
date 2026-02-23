import { pool } from "../src/db.js";

//PTK
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

//SEKOLAH
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

//PESERTA
const getAllPeserta = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;

    const dataQuery = `
      SELECT *
      FROM public.peserta
      ORDER BY peserta_id ASC
      LIMIT $1 OFFSET $2
    `;

    const countQuery = `SELECT COUNT(*) FROM public.peserta`;

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
    console.error("GET ALL PESERTA ERROR:", err);
    res.status(500).json({ message: "Gagal memproses data peserta" });
  }
};

const getPesertaDetail = async (req, res) => {
  try {
    const { id } = req.params;

    const query = `
      SELECT *
      FROM public.peserta
      WHERE peserta_id = $1
    `;

    const result = await pool.query(query, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Peserta tidak ditemukan" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error("GET PESERTA DETAIL ERROR:", err);
    res.status(500).json({ message: "Gagal mengambil detail peserta" });
  }
};

const searchPeserta = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const search = req.query.query || "";
    const offset = (page - 1) * limit;

    const dataQuery = `
      SELECT *
      FROM public.peserta
      WHERE nama ILIKE $1
      ORDER BY nama ASC
      LIMIT $2 OFFSET $3
    `;

    const countQuery = `
      SELECT COUNT(*) FROM public.peserta
      WHERE nama ILIKE $1
    `;

    const searchParam = `%${search}%`;

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
    console.error("SEARCH PESERTA ERROR:", err);
    res.status(500).json({ message: "Gagal mencari peserta" });
  }
};

const deletePeserta = async (req, res) => {
  try {
    const { id } = req.params;

    const result = await pool.query("DELETE FROM public.peserta WHERE peserta_id = $1 RETURNING *", [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Peserta tidak ditemukan" });
    }

    res.json({ message: "Peserta berhasil dihapus" });
  } catch (err) {
    console.error("DELETE PESERTA ERROR:", err);
    res.status(500).json({ message: "Gagal menghapus peserta" });
  }
};

const deleteAllPeserta = async (req, res) => {
  try {
    const role = req.user.role;

    // 1. Validasi Role (Admin & Super Admin)
    if (role !== "admin" && role !== "super admin") {
      return res.status(403).json({
        message: "Akses ditolak: Anda tidak memiliki akses",
      });
    }

    // 2. Cek apakah data sudah kosong
    const checkData = await pool.query("SELECT COUNT(*) FROM public.peserta");
    const totalData = parseInt(checkData.rows[0].count);

    if (totalData === 0) {
      return res.status(400).json({
        message: "data sudah kosong",
      });
    }

    // 3. Eksekusi Penghapusan
    await pool.query("TRUNCATE TABLE public.peserta RESTART IDENTITY CASCADE");

    res.json({
      message: `data berhasil dihapus`,
    });
  } catch (err) {
    console.error("DELETE ALL PESERTA ERROR:", err);
    res.status(500).json({ message: "gagal menghapus data" });
  }
};

//PPG
const getAllPPG = async (req, res) => {
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

    res.json({
      page,
      limit,
      totalData,
      totalPages,
      data: dataResult.rows,
    });
  } catch (err) {
    console.error("PPG ERROR:", err);
    res.status(500).json({ message: "Gagal memproses data PPG" });
  }
};

const searchPPG = async (req, res) => {
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

const deleteAllPPG = async (req, res) => {
  try {
    // 1. Cek apakah tabel sudah kosong
    const checkData = await pool.query("SELECT COUNT(*) FROM public.ppg");
    const count = parseInt(checkData.rows[0].count);

    if (count === 0) {
      return res.status(400).json({
        message: "Data sudah kosong",
      });
    }

    // 2. Jika ada data, baru jalankan TRUNCATE
    await pool.query("TRUNCATE TABLE public.ppg RESTART IDENTITY CASCADE");

    res.json({
      message: `Data berhasil dihapus`,
    });
  } catch (err) {
    console.error("DELETE ALL PPG ERROR:", err);
    res.status(500).json({ message: "Gagal menghapus data" });
  }
};

//KEGIATAN
const insertKegiatan = async (req, res) => {
  try {
    const users_id = req.user.id;
    const role = req.user.role;

    if (role !== "admin" && role !== "super_admin") {
      return res.status(403).json({
        message: "Tidak punya akses membuat kegiatan",
      });
    }

    const { nama_kegiatan, tempat_pelaksanaan, sasaran_peserta, total_peserta, tanggal_pelaksanaan, jenjang_peserta, pendidikan_terakhir } = req.body;

    const parsedSasaran = parseInt(sasaran_peserta);
    const parsedTotal = parseInt(total_peserta);

    if (parsedTotal > parsedSasaran) {
      return res.status(400).json({
        message: "Total peserta tidak boleh melebihi sasaran peserta",
      });
    }

    const result = await pool.query(
      `INSERT INTO public.kegiatan
       (users_id, nama_kegiatan, tempat_pelaksanaan, sasaran_peserta, total_peserta, tanggal_pelaksanaan, jenjang_peserta, pendidikan_terakhir)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       RETURNING *`,
      [users_id, nama_kegiatan, tempat_pelaksanaan, parsedSasaran, parsedTotal, tanggal_pelaksanaan, jenjang_peserta, pendidikan_terakhir],
    );

    res.status(201).json({
      message: "Kegiatan berhasil ditambahkan",
      data: result.rows[0],
    });
  } catch (err) {
    console.error("INSERT KEGIATAN ERROR:", err);
    res.status(500).json({ message: "Gagal menambahkan data kegiatan" });
  }
};

const getAllKegiatan = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const offset = (page - 1) * limit;

    const dataQuery = `
      SELECT 
        k.id,
        k.nama_kegiatan,
        k.tempat_pelaksanaan,
        k.sasaran_peserta,
        k.total_peserta,
        k.tanggal_pelaksanaan,
        k.jenjang_peserta,
        k.pendidikan_terakhir,
        k.created_at,
        u.nama AS created_by
      FROM public.kegiatan k
      JOIN public.users u ON k.users_id = u.id
      ORDER BY k.created_at DESC
      LIMIT $1 OFFSET $2
    `;

    const countQuery = `SELECT COUNT(*) FROM public.kegiatan`;

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
    console.error("GET KEGIATAN ERROR:", err);
    res.status(500).json({ message: "Gagal memproses data kegiatan" });
  }
};

const getKegiatanById = async (req, res) => {
  try {
    const id = parseInt(req.params.id);

    if (isNaN(id)) {
      return res.status(400).json({ message: "ID tidak valid" });
    }

    const query = `
      SELECT 
        k.id,
        k.nama_kegiatan,
        k.tempat_pelaksanaan,
        k.sasaran_peserta,
        k.total_peserta,
        k.tanggal_pelaksanaan,
        k.jenjang_peserta,
        k.pendidikan_terakhir,
        k.created_at,
        u.nama AS created_by
      FROM public.kegiatan k
      JOIN public.users u ON k.users_id = u.id
      WHERE k.id = $1
    `;

    const result = await pool.query(query, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Kegiatan tidak ditemukan" });
    }

    res.json(result.rows[0]);
  } catch (err) {
    console.error("GET KEGIATAN BY ID ERROR:", err);
    res.status(500).json({ message: "Gagal mengambil detail kegiatan" });
  }
};

const searchKegiatanByName = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const search = req.query.query || "";
    const offset = (page - 1) * limit;

    const dataQuery = `
      SELECT 
        k.id,
        k.nama_kegiatan,
        k.tempat_pelaksanaan,
        k.sasaran_peserta,
        k.total_peserta,
        k.tanggal_pelaksanaan,
        k.jenjang_peserta,
        k.pendidikan_terakhir,
        k.created_at,
        u.nama AS created_by
      FROM public.kegiatan k
      JOIN public.users u ON k.users_id = u.id
      WHERE k.nama_kegiatan ILIKE $1
      ORDER BY k.created_at DESC
      LIMIT $2 OFFSET $3
    `;

    const countQuery = `
      SELECT COUNT(*)
      FROM public.kegiatan
      WHERE nama_kegiatan ILIKE $1
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
    console.error("SEARCH KEGIATAN ERROR:", err);
    res.status(500).json({ message: "Gagal mencari data kegiatan" });
  }
};

const deleteKegiatanById = async (req, res) => {
  try {
    const id = parseInt(req.params.id);

    if (isNaN(id)) {
      return res.status(400).json({ message: "ID tidak valid" });
    }

    const role = req.user.role;
    if (role !== "admin") {
      return res.status(403).json({ message: "Tidak punya akses menghapus kegiatan" });
    }

    const deleteQuery = `
      DELETE FROM public.kegiatan
      WHERE id = $1
      RETURNING *
    `;

    const result = await pool.query(deleteQuery, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({ message: "Kegiatan tidak ditemukan" });
    }

    res.json({ message: "Kegiatan berhasil dihapus" });
  } catch (err) {
    console.error("DELETE KEGIATAN ERROR:", err);
    res.status(500).json({ message: "Gagal menghapus kegiatan" });
  }
};

const deleteAllKegiatan = async (req, res) => {
  try {
    const role = req.user.role;

    // 1. Validasi Role (Admin & Super Admin)
    if (role !== "admin" && role !== "super admin") {
      return res.status(403).json({ message: "Akses ini hanya untuk admin dan super admin" });
    }

    // 2. Cek apakah data sudah kosong sebelum TRUNCATE
    const checkData = await pool.query("SELECT COUNT(*) FROM public.kegiatan");
    const totalData = parseInt(checkData.rows[0].count);

    if (totalData === 0) {
      return res.status(400).json({
        message: "Data sudah kosong",
      });
    }

    // 3. Eksekusi Penghapusan
    await pool.query("TRUNCATE TABLE public.kegiatan RESTART IDENTITY CASCADE");

    res.json({
      message: `data berhasil dihapus`,
    });
  } catch (err) {
    console.error("DELETE ALL KEGIATAN ERROR:", err);
    res.status(500).json({ message: "Gagal menghapus data" });
  }
};

export {
  getPTK,
  getSekolah,
  deleteAllPtk,
  deleteAllSekolah,
  searchPTK,
  searchSekolah,
  getSekolahDetail,
  getAllPeserta,
  getPesertaDetail,
  searchPeserta,
  deletePeserta,
  deleteAllPeserta,
  getAllPPG,
  searchPPG,
  deleteAllPPG,
  insertKegiatan,
  getAllKegiatan,
  getKegiatanById,
  searchKegiatanByName,
  deleteKegiatanById,
  deleteAllKegiatan,
};
