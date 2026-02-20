import jwt from "jsonwebtoken";
import { pool } from "../src/db.js";
import path from "path";
import fs from "fs";
import ExcelJS from "exceljs";
import pkg from "pg-copy-streams";
import pgCopyStreams from "pg-copy-streams";
const { from: copyFrom } = pgCopyStreams;

export const insertKegiatan = async (req, res) => {
  try {
    const users_id = req.user.id;
    const role = req.user.role;

    if (role !== "admin" && role !== "super_admin") {
      return res.status(403).json({
        message: "Tidak punya akses membuat kegiatan",
      });
    }

    const {
      nama_kegiatan,
      tempat_pelaksanaan,
      sasaran_peserta,
      total_peserta,
      tanggal_pelaksanaan,
      jenjang_peserta,
      pendidikan_terakhir,
    } = req.body;

    const parsedSasaran = parseInt(sasaran_peserta);
    const parsedTotal = parseInt(total_peserta);

    if (parsedTotal > parsedSasaran) {
      return res.status(400).json({
        message: "Total peserta tidak boleh melebihi sasaran peserta",
      });
    }

    const result = await pool.query(
      `INSERT INTO kegiatan
       (users_id, nama_kegiatan, tempat_pelaksanaan, sasaran_peserta, total_peserta, tanggal_pelaksanaan, jenjang_peserta, pendidikan_terakhir)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       RETURNING *`,
      [
        users_id,
        nama_kegiatan,
        tempat_pelaksanaan,
        parsedSasaran,
        parsedTotal,
        tanggal_pelaksanaan,
        jenjang_peserta,
        pendidikan_terakhir,
      ],
    );

    res.status(201).json({
      message: "Kegiatan berhasil ditambahkan",
      data: result.rows[0],
    });
  } catch (err) {
    console.error("KEGIATAN ERROR:", err);
    res.status(500).json({ message: "Terjadi kesalahan server" });
  }
};

export const getAllKegiatan = async (req, res) => {
  try {
    // pagination
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
      FROM kegiatan k
      JOIN users u ON k.users_id = u.id
      ORDER BY k.created_at DESC
      LIMIT $1 OFFSET $2
    `;

    const countQuery = `
      SELECT COUNT(*) FROM kegiatan
    `;

    const [dataResult, countResult] = await Promise.all([
      pool.query(dataQuery, [limit, offset]),
      pool.query(countQuery),
    ]);

    const totalData = parseInt(countResult.rows[0].count);
    const totalPages = Math.ceil(totalData / limit);

    res.status(200).json({
      page,
      limit,
      totalData,
      totalPages,
      data: dataResult.rows,
    });
  } catch (error) {
    console.error("GET KEGIATAN ERROR:", error);
    res.status(500).json({
      message: "Terjadi kesalahan server",
    });
  }
};

export const getKegiatanById = async (req, res) => {
  try {
    const id = parseInt(req.params.id);

    // validasi id
    if (isNaN(id)) {
      return res.status(400).json({
        message: "ID tidak valid",
      });
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
      FROM kegiatan k
      JOIN users u ON k.users_id = u.id
      WHERE k.id = $1
    `;

    const result = await pool.query(query, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({
        message: "Kegiatan tidak ditemukan",
      });
    }

    res.status(200).json({
      data: result.rows[0],
    });
  } catch (error) {
    console.error("GET KEGIATAN BY ID ERROR:", error);
    res.status(500).json({
      message: "Terjadi kesalahan server",
    });
  }
};

export const searchKegiatanByName = async (req, res) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const search = req.query.query || "";
    const offset = (page - 1) * limit;

    const searchParam = `%${search.trim()}%`;

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
      FROM kegiatan k
      JOIN users u ON k.users_id = u.id
      WHERE k.nama_kegiatan ILIKE $1
      ORDER BY k.created_at DESC
      LIMIT $2 OFFSET $3
    `;

    const countQuery = `
      SELECT COUNT(*)
      FROM kegiatan
      WHERE nama_kegiatan ILIKE $1
    `;

    const [dataResult, countResult] = await Promise.all([
      pool.query(dataQuery, [searchParam, limit, offset]),
      pool.query(countQuery, [searchParam]),
    ]);

    const totalData = parseInt(countResult.rows[0].count);
    const totalPages = Math.ceil(totalData / limit);

    res.status(200).json({
      page,
      limit,
      totalData,
      totalPages,
      data: dataResult.rows,
    });
  } catch (error) {
    console.error("SEARCH KEGIATAN ERROR:", error);
    res.status(500).json({
      message: "Terjadi kesalahan server",
    });
  }
};
export const deleteKegiatanById = async (req, res) => {
  try {
    const id = parseInt(req.params.id);

    // validasi id
    if (isNaN(id)) {
      return res.status(400).json({
        message: "ID tidak valid",
      });
    }

    const role = req.user.role;

    // hanya admin & super_admin
    if (role !== "admin") {
      return res.status(403).json({
        message: "Tidak punya akses menghapus kegiatan",
      });
    }

    const deleteQuery = `
      DELETE FROM kegiatan
      WHERE id = $1
      RETURNING *
    `;

    const result = await pool.query(deleteQuery, [id]);

    if (result.rows.length === 0) {
      return res.status(404).json({
        message: "Kegiatan tidak ditemukan",
      });
    }

    res.status(200).json({
      message: "Kegiatan berhasil dihapus",
      data: result.rows[0],
    });
  } catch (error) {
    console.error("DELETE KEGIATAN ERROR:", error);
    res.status(500).json({
      message: "Terjadi kesalahan server",
    });
  }
};

export const deleteAllKegiatan = async (req, res) => {
  try {
    // ambil role dari middleware
    const role = req.user.role;

    // hanya super_admin yang boleh
    if (role !== "admin") {
      return res.status(403).json({
        message: "Tidak punya akses menghapus semua kegiatan",
      });
    }

    const result = await pool.query("DELETE FROM kegiatan RETURNING *");

    res.status(200).json({
      message: `${result.rowCount} kegiatan berhasil dihapus`,
      totalDeleted: result.rowCount,
      data: result.rows,
    });
  } catch (error) {
    console.error("DELETE ALL KEGIATAN ERROR:", error);
    res.status(500).json({
      message: "Terjadi kesalahan server",
    });
  }
};

const waitFinish = (stream) =>
  new Promise((resolve, reject) => {
    stream.on("finish", resolve);
    stream.on("error", reject);
  });

export const uploadKegiatan = async (req, res) => {
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
      message: "Upload kegiatan berhasil (COPY + INSERT)",
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
