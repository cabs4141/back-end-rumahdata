import fs from "fs";
import path from "path";
import { pool } from "../../config/db.js";
import ExcelJS from "exceljs";
import pgCopyStreams from "pg-copy-streams";
import readline from "readline";
import csvParser from "csv-parser";

const { from: copyFrom } = pgCopyStreams;

/* helper */
const waitFinish = (stream) =>
  new Promise((resolve, reject) => {
    stream.on("finish", resolve);
    stream.on("error", reject);
  });

/* `processFileToCsv` reads XLSX or CSV, extracts valid columns matching `expectedCols`, and generates a clean CSV */
const processFileToCsv = async (filePath, ext, expectedColumns, tempCsv) => {
  const csvStream = fs.createWriteStream(tempCsv);
  let validColumns = [];

  if (ext === ".xlsx") {
    const workbook = new ExcelJS.stream.xlsx.WorkbookReader(filePath);
    let headerParsed = false;
    let headerIndexes = [];

    for await (const sheet of workbook) {
      for await (const row of sheet) {
        if (!headerParsed) {
          const rawHeaders = row.values.slice(1).map((h) =>
            String(h || "")
              .trim()
              .toLowerCase()
              .replace(/\s+/g, "_"),
          );

          rawHeaders.forEach((h, i) => {
            if (expectedColumns.includes(h)) {
              headerIndexes.push({ index: i + 1, colName: h });
              validColumns.push(h);
            }
          });

          if (validColumns.length === 0)
            throw new Error("Tidak ada kolom yang valid pada file.");

          csvStream.write(validColumns.join(",") + "\n");
          headerParsed = true;
          continue;
        }

        const line = headerIndexes
          .map(({ index }) => {
            let cell = row.getCell(index).value;
            let cellStr = "";

            if (cell === null || cell === undefined || cell === "") {
              cellStr = "";
            } else if (typeof cell === "object") {
              cellStr =
                cell.text ||
                cell.result ||
                cell.richText?.map((rt) => rt.text).join("") ||
                "";
            } else {
              cellStr = String(cell);
            }

            return `"${cellStr.replace(/"/g, '""').trim()}"`;
          })
          .join(",");

        csvStream.write(line + "\n");
      }
      break;
    }
    csvStream.end();
    await waitFinish(csvStream);
    return validColumns;
  } else {
    // Determine delimiter
    let delimiter = ",";
    const headerLine = await new Promise((resolve) => {
      const rl = readline.createInterface({
        input: fs.createReadStream(filePath),
      });
      rl.on("line", (line) => {
        rl.close();
        resolve(line);
      });
    });
    if (headerLine && headerLine.includes(";")) delimiter = ";";

    return new Promise((resolve, reject) => {
      let headerMapping = [];

      fs.createReadStream(filePath)
        .pipe(csvParser({ separator: delimiter }))
        .on("headers", (headers) => {
          headers.forEach((h) => {
            const cleanH = String(h || "")
              .trim()
              .toLowerCase()
              .replace(/\s+/g, "_");
            if (expectedColumns.includes(cleanH)) {
              headerMapping.push({ csvKey: h, colName: cleanH });
              validColumns.push(cleanH);
            }
          });

          if (validColumns.length === 0) {
            return reject(new Error("Tidak ada kolom valid pada file"));
          }
          csvStream.write(validColumns.join(",") + "\n");
        })
        .on("data", (row) => {
          const line = headerMapping
            .map(({ csvKey }) => {
              const cell = row[csvKey];
              const cellStr =
                cell === null || cell === undefined ? "" : String(cell);
              return `"${cellStr.replace(/"/g, '""').trim()}"`;
            })
            .join(",");
          csvStream.write(line + "\n");
        })
        .on("end", () => {
          csvStream.end();
          resolve(validColumns);
        })
        .on("error", reject);
    });
  }
};

const uploadSekolah = async (req, res) => {
  const filePath = req.file?.path;
  if (!filePath)
    return res.status(400).json({ message: "File wajib diupload" });

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = filePath + ".formatted.csv";

  const expectedCols = [
    "sekolah_id",
    "semester",
    "nama",
    "nama_nomenklatur",
    "nss",
    "npsn",
    "bentuk_pendidikan",
    "jenjang",
    "alamat_jalan",
    "rt",
    "rw",
    "nama_dusun",
    "kode_desa_kelurahan",
    "desa_kelurahan",
    "kode_kecamatan",
    "kecamatan",
    "kode_kabupaten",
    "kabupaten",
    "kode_provinsi",
    "provinsi",
    "kode_pos",
    "lintang",
    "bujur",
    "nomor_telepon",
    "nomor_fax",
    "email",
    "website",
    "kebutuhan_khusus",
    "status_sekolah",
    "sk_pendirian_sekolah",
    "tanggal_sk_pendirian",
    "status_kepemilikan",
    "yayasan",
    "sk_izin_operasional",
    "tanggal_sk_izin_operasional",
    "no_rekening",
    "nama_bank",
    "cabang_kcp_unit",
    "rekening_atas_nama",
    "mbs",
    "kode_registrasi",
    "npwp",
    "nm_wp",
    "keaktifan",
    "daya_listrik",
    "kontinuitas_listrik",
    "jarak_listrik",
    "wilayah_terpencil",
    "wilayah_perbatasan",
    "wilayah_transmigrasi",
    "wilayah_adat_terpencil",
    "wilayah_bencana_alam",
    "wilayah_bencana_sosial",
    "partisipasi_bos",
    "waktu_penyelenggaraan",
    "sumber_listrik",
    "sertifikasi_iso",
    "akses_internet",
    "akses_internet_2",
    "akreditasi",
    "akreditasi_sp_tmt",
    "akreditasi_sp_sk",
    "luas_tanah_milik",
    "luas_tanah_bukan_milik",
    "angkatan_psp",
    "internet_jenis_layanan",
    "internet_jenis_koneksi",
    "internet_provider",
    "internet_bandwidth",
    "internet_bandwidth_up",
    "internet_bandwidth_down",
    "internet_latency",
    "listrik_sumber",
    "listrik_daya",
    "listrik_kontinuitas",
    "listrik_id_pelanggan",
    "listrik_nomor_meter",
    "listrik_jenis_meter",
    "listrik_status_sambungan",
    "listrik_utama",
  ];

  try {
    const validCols = await processFileToCsv(
      filePath,
      ext,
      expectedCols,
      tempCsv,
    );
    if (!validCols.includes("sekolah_id"))
      throw new Error("Kolom sekolah_id wajib ada pada file");

    await client.query("BEGIN");
    await client.query(`
      DROP TABLE IF EXISTS data_sekolah_staging;
      CREATE TEMP TABLE data_sekolah_staging (LIKE data_sekolah INCLUDING DEFAULTS)
    `);

    const copyStream = client.query(
      copyFrom(`
        COPY data_sekolah_staging (${validCols.join(",")})
        FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
      `),
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    let updateSet = validCols
      .filter((c) => c !== "sekolah_id")
      .map((c) => `${c} = EXCLUDED.${c}`)
      .join(",\n        ");
    if (updateSet) {
      await client.query(`
         INSERT INTO data_sekolah (${validCols.join(",")})
         SELECT DISTINCT ON (sekolah_id) ${validCols.join(",")} FROM data_sekolah_staging
         ORDER BY sekolah_id
         ON CONFLICT (sekolah_id)
         DO UPDATE SET ${updateSet}
       `);
    } else {
      await client.query(`
         INSERT INTO data_sekolah (${validCols.join(",")})
         SELECT DISTINCT ON (sekolah_id) ${validCols.join(",")} FROM data_sekolah_staging
         ORDER BY sekolah_id
         ON CONFLICT (sekolah_id) DO NOTHING
       `);
    }

    await client.query("COMMIT");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    res.json({ message: "Upload data sekolah berhasil (COPY + UPSERT)" });
  } catch (err) {
    await client.query("ROLLBACK");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);
    console.error(err);
    res.status(500).json({ message: err.message });
  } finally {
    client.release();
  }
};

const uploadPtk = async (req, res) => {
  const filePath = req.file?.path;
  if (!filePath)
    return res.status(400).json({ message: "File wajib diupload" });

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = filePath + ".formatted.csv";

  const expectedCols = [
    "ptk_id",
    "semester",
    "sekolah_id",
    "ptk_terdaftar_id",
    "nama",
    "nip",
    "jenis_kelamin",
    "tempat_lahir",
    "tanggal_lahir",
    "nik",
    "no_kk",
    "niy_nigk",
    "nuptk",
    "nuks",
    "status_kepegawaian",
    "jenis_ptk",
    "pengawas_bidang_studi",
    "agama",
    "kewarganegaraan",
    "alamat_jalan",
    "rt",
    "rw",
    "nama_dusun",
    "kode_desa_kelurahan",
    "desa_kelurahan",
    "kode_kecamatan",
    "kecamatan",
    "kode_kabupaten",
    "kabupaten",
    "kode_provinsi",
    "provinsi",
    "kode_pos",
    "lintang",
    "bujur",
    "no_telepon_rumah",
    "email",
    "status_keaktifan",
    "sk_cpns",
    "tgl_cpns",
    "sk_pengangkatan",
    "tmt_pengangkatan",
    "lembaga_pengangkat",
    "pangkat_golongan",
    "keahlian_laboratorium",
    "sumber_gaji",
    "nama_ibu_kandung",
    "status_perkawinan",
    "nama_suami_istri",
    "nip_suami_istri",
    "pekerjaan_suami_istri",
    "tmt_pns",
    "sudah_lisensi_kepala_sekolah",
    "jumlah_sekolah_binaan",
    "pernah_diklat_kepengawasan",
    "nm_wp",
    "status_data",
    "karpeg",
    "karpas",
    "mampu_handle_kk",
    "keahlian_braille",
    "keahlian_bhs_isyarat",
    "npwp",
    "bank",
    "rekening_bank",
    "rekening_atas_nama",
    "tahun_ajaran",
    "nomor_surat_tugas",
    "tanggal_surat_tugas",
    "tmt_tugas",
    "ptk_induk",
    "jenis_keluar",
    "tgl_ptk_keluar",
    "riwayat_kepangkatan_pangkat_golongan",
    "riwayat_kepangkatan_nomor_sk",
    "riwayat_kepangkatan_tanggal_sk",
    "riwayat_kepangkatan_tmt_pangkat",
    "riwayat_kepangkatan_masa_kerja_gol_tahun",
    "riwayat_kepangkatan_masa_kerja_gol_bulan",
    "riwayat_gaji_berkala_pangkat_golongan",
    "riwayat_gaji_berkala_nomor_sk",
    "riwayat_gaji_berkala_tanggal_sk",
    "riwayat_gaji_berkala_tmt_kgb",
    "riwayat_gaji_berkala_masa_kerja_tahun",
    "riwayat_gaji_berkala_masa_kerja_bulan",
    "riwayat_gaji_berkala_gaji_pokok",
    "inpassing_pangkat_golongan",
    "inpassing_no_sk_inpassing",
    "inpassing_tgl_sk_inpassing",
    "inpassing_tmt_inpassing",
    "inpassing_angka_kredit",
    "inpassing_masa_kerja_tahun",
    "inpassing_masa_kerja_bulan",
    "riwayat_sertifikasi_bidang_studi",
    "riwayat_sertifikasi_jenis_sertifikasi",
    "riwayat_sertifikasi_tahun_sertifikasi",
    "riwayat_sertifikasi_nomor_sertifikat",
    "riwayat_sertifikasi_nrg",
    "riwayat_sertifikasi_nomor_peserta",
    "riwayat_pendidikan_formal_bidang_studi",
    "riwayat_pendidikan_formal_jenjang_pendidikan",
    "riwayat_pendidikan_formal_gelar_akademik",
    "riwayat_pendidikan_formal_satuan_pendidikan_formal",
    "riwayat_pendidikan_formal_fakultas",
    "riwayat_pendidikan_formal_kependidikan",
    "riwayat_pendidikan_formal_tahun_masuk",
    "riwayat_pendidikan_formal_tahun_lulus",
    "riwayat_pendidikan_formal_nim",
    "riwayat_pendidikan_formal_status_kuliah",
    "riwayat_pendidikan_formal_semester",
    "riwayat_pendidikan_formal_ipk",
    "jumlah_anak",
    "tugas_tambahan_jabatan_ptk",
    "tugas_tambahan_sekolah",
    "tugas_tambahan_jumlah_jam",
    "tugas_tambahan_nomor_sk",
    "tugas_tambahan_tmt_tambahan",
    "tugas_tambahan_tst_tambahan",
    "riwayat_struktural_jabatan_ptk",
    "riwayat_struktural_sk_struktural",
    "riwayat_struktural_tmt_jabatan",
    "riwayat_fungsional_jabatan_fungsional",
    "riwayat_fungsional_sk_jabfung",
    "riwayat_fungsional_tmt_jabatan",
    "jabatan_ptk",
  ];

  try {
    const validCols = await processFileToCsv(
      filePath,
      ext,
      expectedCols,
      tempCsv,
    );
    if (!validCols.includes("ptk_id"))
      throw new Error("Kolom ptk_id wajib ada pada file");

    await client.query("BEGIN");
    await client.query(`
      DROP TABLE IF EXISTS ptk_staging;
      CREATE TEMP TABLE ptk_staging (LIKE public.ptk INCLUDING DEFAULTS)
    `);

    const copyStream = client.query(
      copyFrom(`
        COPY ptk_staging (${validCols.join(",")})
        FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
      `),
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    let updateSet = validCols
      .filter((c) => c !== "ptk_id")
      .map((c) => `${c} = EXCLUDED.${c}`)
      .join(",\n        ");
    if (updateSet) {
      await client.query(`
         INSERT INTO public.ptk (${validCols.join(",")})
         SELECT DISTINCT ON (ptk_id) ${validCols.join(",")} FROM ptk_staging
         ORDER BY ptk_id
         ON CONFLICT (ptk_id)
         DO UPDATE SET ${updateSet}
       `);
    } else {
      await client.query(`
         INSERT INTO public.ptk (${validCols.join(",")})
         SELECT DISTINCT ON (ptk_id) ${validCols.join(",")} FROM ptk_staging
         ORDER BY ptk_id
         ON CONFLICT (ptk_id) DO NOTHING
       `);
    }

    await client.query("COMMIT");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    res.json({ message: "Upload PTK berhasil (COPY + UPSERT)" });
  } catch (err) {
    await client.query("ROLLBACK");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);
    console.error(err);
    res.status(500).json({ message: err.message });
  } finally {
    client.release();
  }
};

const uploadPeserta = async (req, res) => {
  const filePath = req.file?.path;
  const kegiatan_id = req.body.kegiatan_id;

  if (!filePath)
    return res.status(400).json({ message: "File wajib diupload" });
  if (!kegiatan_id) {
    fs.unlinkSync(filePath);
    return res.status(400).json({ message: "kegiatan_id wajib diisi" });
  }

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = filePath + ".formatted.csv";

  const expectedCols = [
    "nama",
    "kabupaten",
    "instansi",
    "jabatan",
    "alamat",
    "jenjang",
    "peran",
  ];

  try {
    const validCols = await processFileToCsv(
      filePath,
      ext,
      expectedCols,
      tempCsv,
    );

    await client.query("BEGIN");
    await client.query(`
      DROP TABLE IF EXISTS peserta_staging;
      CREATE TEMP TABLE peserta_staging (
        nama text, kabupaten text, instansi text, jabatan text, alamat text, jenjang text, peran text
      )
    `);

    const copyStream = client.query(
      copyFrom(`
        COPY peserta_staging (${validCols.join(",")})
        FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
      `),
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    await client.query(
      `
      INSERT INTO peserta (kegiatan_id, ${validCols.join(",")})
      SELECT $1, ${validCols.map((c) => `TRIM(${c})`).join(", ")}
      FROM peserta_staging
    `,
      [kegiatan_id],
    );

    await client.query("COMMIT");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    res.json({ message: "Upload peserta berhasil" });
  } catch (err) {
    await client.query("ROLLBACK");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);
    console.error("UPLOAD ERROR:", err);
    res.status(500).json({ message: "Gagal memproses file: " + err.message });
  } finally {
    client.release();
  }
};

const uploadPpg = async (req, res) => {
  const filePath = req.file?.path;
  if (!filePath)
    return res.status(400).json({ message: "File wajib diupload" });

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = filePath + ".formatted.csv";

  const expectedCols = [
    "no_ukg",
    "tahun",
    "nama_lengkap",
    "no_hp",
    "nama_sekolah",
    "npsn_sekolah",
    "jenjang_sekolah",
    "provinsi_sekolah",
    "kota_kab_sekolah",
    "status_kesediaan",
    "waktu_isi_kesediaan",
    "kode_bs_ppg",
    "bidang_studi_ppg",
    "lptk",
    "status_plotting",
    "alasan",
    "status_konfirmasi_email",
    "waktu_konfirmasi_email",
    "email_konfirmasi",
    "tahap",
  ];

  try {
    const validCols = await processFileToCsv(
      filePath,
      ext,
      expectedCols,
      tempCsv,
    );
    if (!validCols.includes("no_ukg") || !validCols.includes("tahun")) {
      throw new Error("Kolom no_ukg dan tahun wajib ada pada file");
    }

    await client.query("BEGIN");
    await client.query(`
      DROP TABLE IF EXISTS ppg_staging;
      CREATE TEMP TABLE ppg_staging (LIKE ppg INCLUDING DEFAULTS)
    `);

    const copyStream = client.query(
      copyFrom(`
        COPY ppg_staging (${validCols.join(",")})
        FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
      `),
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    let updateSet = validCols
      .filter((c) => c !== "no_ukg" && c !== "tahun")
      .map((c) => `${c} = EXCLUDED.${c}`)
      .join(",\n        ");
    if (updateSet) {
      await client.query(`
         INSERT INTO ppg (${validCols.join(",")})
         SELECT DISTINCT ON (no_ukg, tahun) ${validCols.join(",")} FROM ppg_staging
         ORDER BY no_ukg, tahun
         ON CONFLICT (no_ukg, tahun)
         DO UPDATE SET ${updateSet}
       `);
    } else {
      await client.query(`
         INSERT INTO ppg (${validCols.join(",")})
         SELECT DISTINCT ON (no_ukg, tahun) ${validCols.join(",")} FROM ppg_staging
         ORDER BY no_ukg, tahun
         ON CONFLICT (no_ukg, tahun) DO NOTHING
       `);
    }

    await client.query("COMMIT");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    res.json({ message: "Upload data PPG berhasil" });
  } catch (err) {
    await client.query("ROLLBACK");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);
    console.error("UPLOAD ERROR:", err);
    res.status(500).json({ message: "Gagal memproses file: " + err.message });
  } finally {
    client.release();
  }
};

const uploadKegiatan = async (req, res) => {
  const filePath = req.file?.path;
  if (!filePath)
    return res.status(400).json({ message: "File wajib diupload" });

  const ext = path.extname(req.file.originalname).toLowerCase();
  const client = await pool.connect();
  const tempCsv = filePath + ".formatted.csv";

  const expectedCols = [
    "nama_kegiatan",
    "tanggal_mulai",
    "tanggal_selesai",
    "penanggung_jawab",
    "team_id",
    "tempat_pelaksanaan",
    "tahun",
    "sasaran_peserta",
    "total_peserta",
  ];

  try {
    const validCols = await processFileToCsv(
      filePath,
      ext,
      expectedCols,
      tempCsv,
    );

    await client.query("BEGIN");
    await client.query(`
      DROP TABLE IF EXISTS kegiatan_staging;
      CREATE TEMP TABLE kegiatan_staging
      (
        nama_kegiatan TEXT, tanggal_mulai TEXT, tanggal_selesai TEXT, penanggung_jawab TEXT,
        team_id TEXT, tempat_pelaksanaan TEXT, tahun TEXT, sasaran_peserta TEXT, total_peserta TEXT
      )
    `);

    const copyStream = client.query(
      copyFrom(`
        COPY kegiatan_staging (${validCols.join(",")})
        FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')
      `),
    );

    fs.createReadStream(tempCsv).pipe(copyStream);
    await waitFinish(copyStream);

    let selectFields = validCols.map((c) => {
      if (c === "tanggal_mulai" || c === "tanggal_selesai") {
        return `(
          CASE
            WHEN TRIM(${c}) ~ '^[0-9]+$' THEN DATE '1899-12-30' + TRIM(${c})::INT
            WHEN TRIM(${c}) = '' THEN NULL
            WHEN TRIM(${c}) ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN TRIM(${c})::DATE
            ELSE NULL
          END
         )::DATE`;
      }
      if (
        c === "team_id" ||
        c === "tahun" ||
        c === "sasaran_peserta" ||
        c === "total_peserta"
      ) {
        return `NULLIF(TRIM(${c}), '')::INT`;
      }
      return `TRIM(${c})`;
    });

    await client.query(
      `
      INSERT INTO kegiatan (users_id, created_at, updated_at, ${validCols.join(",")})
      SELECT $1, NOW(), NOW(), ${selectFields.join(", ")}
      FROM kegiatan_staging
    `,
      [req.user.id],
    );

    await client.query("COMMIT");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);

    res.json({ message: "Upload berhasil" });
  } catch (err) {
    await client.query("ROLLBACK");
    fs.existsSync(filePath) && fs.unlinkSync(filePath);
    fs.existsSync(tempCsv) && fs.unlinkSync(tempCsv);
    console.error(err);
    res.status(500).json({ message: err.message });
  } finally {
    client.release();
  }
};

export { uploadPtk, uploadSekolah, uploadPeserta, uploadPpg, uploadKegiatan };
