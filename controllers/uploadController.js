import fs from "fs";
import path from "path";
import { pool } from "../src/db.js";
import ExcelJS from "exceljs";
import pgCopyStreams from "pg-copy-streams";
const { from: copyFrom } = pgCopyStreams;

/* helper */
const waitFinish = (stream) =>
  new Promise((resolve, reject) => {
    stream.on("finish", resolve);
    stream.on("error", reject);
  });

/* helper ambil value cell supaya tidak [object Object] */
function extractValue(cell) {
  if (!cell) return "";

  const raw = cell.value;

  if (raw === null || raw === undefined) return "";

  if (typeof raw === "object") {
    if (raw.richText) {
      return raw.richText.map((r) => r.text).join("");
    }
    if (raw.text) {
      return raw.text;
    }
    if (raw.result) {
      return raw.result;
    }
    return String(raw);
  }

  return String(raw);
}

const uploadSekolah = async (req, res) => {
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
      `),
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

const uploadPtk = async (req, res) => {
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
      `),
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

const uploadPeserta = async (req, res) => {
  const filePath = req.file?.path;
  const kegiatan_id = req.body.kegiatan_id;

  if (!filePath) {
    return res.status(400).json({ message: "File wajib diupload" });
  }

  if (!kegiatan_id) {
    return res.status(400).json({ message: "kegiatan_id wajib diisi" });
  }

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = ext === ".xlsx" ? filePath + ".csv" : filePath;

  try {
    /* =========================
       1. XLSX → CSV (STREAM SAFE)
    ========================= */
    if (ext === ".xlsx") {
      const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
      const csv = fs.createWriteStream(tempCsv);

      for await (const sheet of workbook) {
        let headers = [];

        for await (const row of sheet) {
          // HEADER
          if (row.number === 1) {
            headers = row.values.slice(1).map((h) => String(h || "").trim());

            csv.write(headers.join(",") + ",kegiatan_id\n");
            continue;
          }

          // DATA
          const line = headers
            .map((_, i) => {
              const cell = row.getCell(i + 1);
              const value = extractValue(cell);

              return `"${value.replace(/"/g, '""')}"`;
            })
            .join(",");

          csv.write(line + `,"${kegiatan_id}"\n`);
        }

        break; // hanya sheet pertama
      }

      csv.end();
      await waitFinish(csv);
    }

    /* =========================
       2. COPY KE TEMP TABLE
    ========================= */
    await client.query("BEGIN");

    await client.query(`
      CREATE TEMP TABLE peserta_staging (
        nama text,
        kabupaten text,
        instansi text,
        jabatan text,
        alamat text
      )
    `);

    const copyStream = client.query(
      copyFrom(`
    COPY peserta_staging (nama, kabupaten, instansi, jabatan, alamat)
    FROM STDIN
    WITH (
      FORMAT csv,
      HEADER true,
      DELIMITER ';',
      ENCODING 'UTF8'
    )
  `),
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    /* =========================
       3. INSERT KE TABEL PESERTA
    ========================= */
    await client.query(
      `
    INSERT INTO peserta (nama, kabupaten, instansi, jabatan, alamat, kegiatan_id)
    SELECT 
      TRIM(nama),
      TRIM(kabupaten),
      TRIM(instansi),
      TRIM(jabatan),
      TRIM(alamat),
      $1
    FROM peserta_staging
  `,
      [kegiatan_id],
    );

    await client.query("COMMIT");

    /* =========================
       4. CLEAN FILE
    ========================= */
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    ext === ".xlsx" && fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    res.json({
      message: "Upload peserta berhasil (COPY)",
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

const uploadPpg = async (req, res) => {
  const filePath = req.file?.path;

  if (!filePath) return res.status(400).json({ message: "File wajib diupload" });

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = ext === ".xlsx" ? filePath + ".csv" : filePath;

  try {
    /* XLSX → CSV */
    if (ext === ".xlsx") {
      const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
      const csv = fs.createWriteStream(tempCsv);

      for await (const sheet of workbook) {
        let headers = [];

        for await (const row of sheet) {
          if (row.number === 1) {
            headers = row.values.slice(1).map((h) => String(h).toLowerCase().replace(/\s+/g, "_"));

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

    /* COPY */
    await client.query("BEGIN");

    await client.query(`
        CREATE TEMP TABLE ppg_staging
        (LIKE ppg INCLUDING DEFAULTS)
        `);

    const copyStream = client.query(
      copyFrom(`
            COPY ppg_staging
            FROM STDIN
            WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
        `),
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    /* UPSERT */
    await client.query(`
        INSERT INTO ppg
        SELECT DISTINCT ON (no_ukg) *
        FROM ppg_staging
        ORDER BY no_ukg
        ON CONFLICT (no_ukg)
        DO UPDATE SET
            nama_lengkap              = EXCLUDED.nama_lengkap,
            no_hp                     = EXCLUDED.no_hp,
            nama_sekolah              = EXCLUDED.nama_sekolah,
            npsn_sekolah              = EXCLUDED.npsn_sekolah,
            jenjang_sekolah           = EXCLUDED.jenjang_sekolah,
            provinsi_sekolah          = EXCLUDED.provinsi_sekolah,
            kota_kab_sekolah          = EXCLUDED.kota_kab_sekolah,
            status_kesediaan          = EXCLUDED.status_kesediaan,
            waktu_isi_kesediaan       = EXCLUDED.waktu_isi_kesediaan,
            kode_bs_ppg               = EXCLUDED.kode_bs_ppg,
            bidang_studi_ppg          = EXCLUDED.bidang_studi_ppg,
            lptk                      = EXCLUDED.lptk,
            status_plotting           = EXCLUDED.status_plotting,
            alasan                    = EXCLUDED.alasan,
            status_konfirmasi_email   = EXCLUDED.status_konfirmasi_email,
            waktu_konfirmasi_email    = EXCLUDED.waktu_konfirmasi_email,
            email_konfirmasi          = EXCLUDED.email_konfirmasi,
            tahap                     = EXCLUDED.tahap
        `);

    await client.query("COMMIT");

    fs.unlinkSync(filePath);
    ext === ".xlsx" && fs.unlinkSync(tempCsv);

    res.json({ message: "Upload data PPG berhasil" });
  } catch (err) {
    await client.query("ROLLBACK");
    res.status(500).json({ message: err.message });
  } finally {
    client.release();
  }
};

const uploadKegiatan = async (req, res) => {
  const filePath = req.file?.path;

  if (!filePath) {
    return res.status(400).json({ message: "File wajib diupload" });
  }

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = ext === ".xlsx" ? filePath + ".csv" : filePath;

  try {
    /* =========================
       1️⃣ XLSX → CSV (STREAM)
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
              return `"${String(cell).replace(/"/g, '""').trim()}"`;
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
       2️⃣ COPY → STAGING TABLE
    ========================= */
    await client.query("BEGIN");

    await client.query(`
      DROP TABLE IF EXISTS kegiatan_staging;
      CREATE TEMP TABLE kegiatan_staging
      (
        nama_kegiatan TEXT,
        tempat_pelaksanaan TEXT,
        sasaran_peserta TEXT,
        total_peserta TEXT,
        tanggal_pelaksanaan TEXT,
        jenjang_peserta TEXT,
        pendidikan_terakhir TEXT
      )
    `);

    const copyStream = client.query(
      copyFrom(`
        COPY kegiatan_staging
        FROM STDIN
        WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
      `),
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    /* =========================
       3️⃣ INSERT KE kegiatan (SMART DATE HANDLER)
    ========================= */
    await client.query(
      `
      INSERT INTO kegiatan (
        users_id,
        nama_kegiatan,
        tempat_pelaksanaan,
        sasaran_peserta,
        total_peserta,
        tanggal_pelaksanaan,
        jenjang_peserta,
        pendidikan_terakhir,
        created_at,
        updated_at
      )
      SELECT
        $1,
        TRIM(nama_kegiatan),
        TRIM(tempat_pelaksanaan),
        NULLIF(TRIM(sasaran_peserta), '')::INT,
        NULLIF(TRIM(total_peserta), '')::INT,

        (
          CASE
            -- Excel serial number
            WHEN TRIM(tanggal_pelaksanaan) ~ '^[0-9]+$'
              THEN DATE '1899-12-30' + TRIM(tanggal_pelaksanaan)::INT

            -- kosong
            WHEN TRIM(tanggal_pelaksanaan) = ''
              THEN NULL

            -- ISO date string (YYYY-MM-DD)
            WHEN TRIM(tanggal_pelaksanaan) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
              THEN TRIM(tanggal_pelaksanaan)::DATE

            -- fallback aman
            ELSE NULL
          END
        )::DATE,

        TRIM(jenjang_peserta),
        TRIM(pendidikan_terakhir),
        NOW(),
        NOW()
      FROM kegiatan_staging
      `,
      [req.user.id],
    );

    await client.query("COMMIT");

    /* =========================
       4️⃣ CLEAN FILE
    ========================= */
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    ext === ".xlsx" && fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    res.json({
      message: "Upload berhasil",
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

export { uploadPtk, uploadSekolah, uploadPeserta, uploadPpg, uploadKegiatan };
