import XLSX from "xlsx";
import fs from "fs";
import path from "path";
import { pool } from "../src/db.js";
import ExcelJS from "exceljs";

import pgCopyStreams from "pg-copy-streams";
const { from: copyFrom } = pgCopyStreams;

// Show All Data
export const showAllData = async (req, res) => {
  try {
    const query = "SELECT * FROM data_sekolah LIMIT 3";
    const result = await pool.query(query);
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).json({ message: "Internal server error" });
  }
};

/* helper */
const waitFinish = (stream) =>
  new Promise((resolve, reject) => {
    stream.on("finish", resolve);
    stream.on("error", reject);
  });

export const insertExcelToDBDataSekolah = async (req, res) => {
  const filePath = req.file?.path;
  if (!filePath) {
    return res.status(400).json({ message: "File wajib diupload" });
  }

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = ext === ".xlsx" ? filePath + ".csv" : filePath;

  try {
    /* =========================
       1. XLSX → CSV (STREAM)
    ========================= */
    if (ext === ".xlsx") {
      const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
      const csv = fs.createWriteStream(tempCsv);

      for await (const sheet of workbook) {
        let headers = [];

        for await (const row of sheet) {
          if (row.number === 1) {
            headers = row.values.slice(1);
            csv.write(headers.join(",") + "\n");
            continue;
          }

          const line = headers
            .map((_, i) => {
              const cell = row.getCell(i + 1).value ?? "";
              return `"${String(cell).replace(/"/g, '""')}"`;
            })
            .join(",");

          csv.write(line + "\n");
        }
        break;
      }

      csv.end();
      await waitFinish(csv);
    }

    /* =========================
       2. COPY → STAGING TABLE
    ========================= */
    await client.query("BEGIN");

    await client.query(`
      DROP TABLE IF EXISTS data_sekolah_staging;
      CREATE TEMP TABLE data_sekolah_staging
      (LIKE data_sekolah INCLUDING DEFAULTS)
    `);

    const copyStream = client.query(
      copyFrom(`
        COPY data_sekolah_staging
        FROM STDIN
        WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
      `)
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    /* =========================
       3. UPSERT KE data_sekolah
    ========================= */
    await client.query(`
      INSERT INTO data_sekolah
      SELECT DISTINCT ON (sekolah_id) *
      FROM data_sekolah_staging
      ORDER BY sekolah_id
      ON CONFLICT (sekolah_id)
      DO UPDATE SET
        nama                          = EXCLUDED.nama,
        npsn                          = EXCLUDED.npsn,
        bentuk_pendidikan             = EXCLUDED.bentuk_pendidikan,
        jenjang                       = EXCLUDED.jenjang,
        alamat_jalan                  = EXCLUDED.alamat_jalan,
        kode_desa_kelurahan           = EXCLUDED.kode_desa_kelurahan,
        desa_kelurahan                = EXCLUDED.desa_kelurahan,
        kode_kecamatan                = EXCLUDED.kode_kecamatan,
        kecamatan                     = EXCLUDED.kecamatan,
        kode_kabupaten                = EXCLUDED.kode_kabupaten,
        kabupaten                     = EXCLUDED.kabupaten,
        kode_provinsi                 = EXCLUDED.kode_provinsi,
        provinsi                      = EXCLUDED.provinsi,
        kode_pos                      = EXCLUDED.kode_pos,
        email                         = EXCLUDED.email,
        kebutuhan_khusus              = EXCLUDED.kebutuhan_khusus,
        status_sekolah                = EXCLUDED.status_sekolah,
        sk_pendirian_sekolah          = EXCLUDED.sk_pendirian_sekolah,
        tanggal_sk_pendirian          = EXCLUDED.tanggal_sk_pendirian,
        status_kepemilikan            = EXCLUDED.status_kepemilikan,
        yayasan                       = EXCLUDED.yayasan,
        sk_izin_operasional           = EXCLUDED.sk_izin_operasional,
        tanggal_sk_izin_operasional   = EXCLUDED.tanggal_sk_izin_operasional,
        rekening_atas_nama            = EXCLUDED.rekening_atas_nama,
        mbs                           = EXCLUDED.mbs,
        kode_registrasi               = EXCLUDED.kode_registrasi,
        npwp                          = EXCLUDED.npwp,
        nm_wp                         = EXCLUDED.nm_wp,
        keaktifan                     = EXCLUDED.keaktifan,
        wilayah_terpencil             = EXCLUDED.wilayah_terpencil,
        wilayah_perbatasan            = EXCLUDED.wilayah_perbatasan,
        wilayah_transmigrasi          = EXCLUDED.wilayah_transmigrasi,
        wilayah_adat_terpencil        = EXCLUDED.wilayah_adat_terpencil,
        wilayah_bencana_alam          = EXCLUDED.wilayah_bencana_alam,
        wilayah_bencana_sosial        = EXCLUDED.wilayah_bencana_sosial,
        partisipasi_bos               = EXCLUDED.partisipasi_bos,
        akses_internet                = EXCLUDED.akses_internet,
        akses_internet_2              = EXCLUDED.akses_internet_2,
        akreditasi                    = EXCLUDED.akreditasi,
        akreditasi_sp_tmt             = EXCLUDED.akreditasi_sp_tmt,
        akreditasi_sp_sk              = EXCLUDED.akreditasi_sp_sk,
        luas_tanah_milik              = EXCLUDED.luas_tanah_milik,
        luas_tanah_bukan_milik        = EXCLUDED.luas_tanah_bukan_milik,
        angkatan_psp                  = EXCLUDED.angkatan_psp
    `);

    await client.query("COMMIT");

    /* =========================
       4. CLEAN FILE
    ========================= */
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    ext === ".xlsx" && fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    res.json({
      message: "Upload data sekolah berhasil (COPY + UPSERT)",
    });
  } catch (err) {
    await client.query("ROLLBACK");

    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    ext === ".xlsx" && fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    console.error(err);
    res.status(500).json({ message: err.message });
  } finally {
    client.release();
  }
};

export const insertExcelToDBPTK = async (req, res) => {
  const filePath = req.file?.path;
  if (!filePath) {
    return res.status(400).json({ message: "File wajib diupload" });
  }

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = ext === ".xlsx" ? filePath + ".csv" : filePath;

  try {
    /* =========================
       1. XLSX → CSV (STREAM)
    ========================= */
    if (ext === ".xlsx") {
      const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
      const csv = fs.createWriteStream(tempCsv);

      for await (const sheet of workbook) {
        let headers = [];

        for await (const row of sheet) {
          if (row.number === 1) {
            headers = row.values.slice(1);
            csv.write(headers.join(",") + "\n");
            continue;
          }

          const line = headers
            .map((_, i) => {
              const cell = row.getCell(i + 1).value ?? "";
              return `"${String(cell).replace(/"/g, '""')}"`;
            })
            .join(",");

          csv.write(line + "\n");
        }
        break;
      }

      csv.end();
      await waitFinish(csv);
    }

    /* =========================
       2. COPY → STAGING
    ========================= */
    await client.query("BEGIN");

    await client.query(`
      CREATE TEMP TABLE ptk_staging
      (LIKE public.ptk INCLUDING DEFAULTS)
    `);

    const copyStream = client.query(
      copyFrom(`
        COPY ptk_staging
        FROM STDIN
        WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
      `)
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    /* =========================
       3. UPSERT KE TABEL PTK
    ========================= */
    await client.query(`
      INSERT INTO public.ptk
      SELECT DISTINCT ON (ptk_id) *
      FROM ptk_staging
      ORDER BY ptk_id
      ON CONFLICT (ptk_id)
      DO UPDATE SET
        semester                 = EXCLUDED.semester,
        sekolah_id               = EXCLUDED.sekolah_id,
        ptk_terdaftar_id         = EXCLUDED.ptk_terdaftar_id,
        nama                     = EXCLUDED.nama,
        nip                      = EXCLUDED.nip,
        jenis_kelamin            = EXCLUDED.jenis_kelamin,
        tempat_lahir             = EXCLUDED.tempat_lahir,
        tanggal_lahir            = EXCLUDED.tanggal_lahir,
        nik                      = EXCLUDED.nik,
        no_kk                    = EXCLUDED.no_kk,
        niy_nigk                 = EXCLUDED.niy_nigk,
        status_kepegawaian       = EXCLUDED.status_kepegawaian,
        jenis_ptk                = EXCLUDED.jenis_ptk,
        pengawas_bidang_studi    = EXCLUDED.pengawas_bidang_studi,
        agama                    = EXCLUDED.agama,
        kewarganegaraan          = EXCLUDED.kewarganegaraan,
        alamat_jalan             = EXCLUDED.alamat_jalan,
        rt                       = EXCLUDED.rt,
        rw                       = EXCLUDED.rw,
        nama_dusun               = EXCLUDED.nama_dusun,
        kode_desa_kelurahan      = EXCLUDED.kode_desa_kelurahan,
        desa_kelurahan           = EXCLUDED.desa_kelurahan,
        kode_kecamatan           = EXCLUDED.kode_kecamatan,
        kecamatan                = EXCLUDED.kecamatan,
        kode_kabupaten           = EXCLUDED.kode_kabupaten,
        kabupaten                = EXCLUDED.kabupaten,
        kode_provinsi            = EXCLUDED.kode_provinsi,
        provinsi                 = EXCLUDED.provinsi,
        kode_pos                 = EXCLUDED.kode_pos,
        lintang                  = EXCLUDED.lintang,
        bujur                    = EXCLUDED.bujur,
        no_telepon_rumah         = EXCLUDED.no_telepon_rumah,
        email                    = EXCLUDED.email,
        status_keaktifan         = EXCLUDED.status_keaktifan,
        sk_cpns                  = EXCLUDED.sk_cpns,
        tgl_cpns                 = EXCLUDED.tgl_cpns,
        sk_pengangkatan          = EXCLUDED.sk_pengangkatan,
        tmt_pengangkatan         = EXCLUDED.tmt_pengangkatan,
        lembaga_pengangkat       = EXCLUDED.lembaga_pengangkat,
        pangkat_golongan         = EXCLUDED.pangkat_golongan,
        sumber_gaji              = EXCLUDED.sumber_gaji,
        nama_ibu_kandung         = EXCLUDED.nama_ibu_kandung,
        status_perkawinan        = EXCLUDED.status_perkawinan,
        npwp                     = EXCLUDED.npwp,
        rekening_bank            = EXCLUDED.rekening_bank,
        rekening_atas_nama       = EXCLUDED.rekening_atas_nama,
        tahun_ajaran             = EXCLUDED.tahun_ajaran,
        jabatan_ptk              = EXCLUDED.jabatan_ptk
    `);

    await client.query("COMMIT");

    /* =========================
       4. CLEAN FILE
    ========================= */
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    ext === ".xlsx" && fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    res.json({
      message: "Upload PTK berhasil (COPY + UPSERT)",
    });
  } catch (err) {
    await client.query("ROLLBACK");

    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    ext === ".xlsx" && fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    console.error(err);
    res.status(500).json({ message: err.message });
  } finally {
    client.release();
  }
};

export const showDataSekolahByNama = async (req, res) => {
  try {
    const { nama } = req.query;

    if (!nama) {
      return res
        .status(400)
        .json({ message: 'Query parameter "nama" wajib diisi' });
    }

    const query = `
      SELECT 
          s.sekolah_id,
          s.nama AS nama_sekolah,
          s.npsn,
          s.alamat_jalan,
          s.kecamatan,
          s.kabupaten,
          s.provinsi,
          s.akreditasi,
          COALESCE(
              json_agg(
                  json_build_object(
                      'ptk_id', p.ptk_id,
                      'nama', p.nama,
                      'nip', p.nip,
                      'jenis_kelamin', p.jenis_kelamin,
                      'jabatan_ptk', p.jabatan_ptk,
                      'status_keaktifan', p.status_keaktifan,
                      'email', p.email
                  )
              ) FILTER (WHERE p.ptk_id IS NOT NULL), 
              '[]'
          ) AS daftar_ptk
      FROM 
          data_sekolah s
      LEFT JOIN 
          ptk p
      ON 
           LOWER(s.sekolah_id) = LOWER(p.sekolah_id)
      WHERE 
          s.nama ILIKE $1
      GROUP BY 
          s.sekolah_id, s.nama, s.npsn, s.alamat_jalan, s.kecamatan, s.kabupaten, s.provinsi, s.akreditasi
      ORDER BY 
          s.nama
      LIMIT 3;
    `;

    const result = await pool.query(query, [`%${nama}%`]);
    res.json(result.rows);
  } catch (error) {
    console.error("Error fetching data:", error);
    res.status(500).json({ message: "Internal server error" });
  }
};

import jwt from "jsonwebtoken";

export const verifyToken = (req, res, next) => {
  const authHeader = req.headers.authorization;

  if (!authHeader) {
    return res.status(401).json({ message: "Token tidak ditemukan" });
  }

  const token = authHeader.split(" ")[1];

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    req.user = decoded; // info user bisa dipakai kalau perlu
    next();
  } catch (error) {
    return res.status(403).json({ message: "Token tidak valid" });
  }
};
