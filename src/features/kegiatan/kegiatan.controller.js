import { pool } from "../../config/db.js";

export const insertKegiatan = async (req, res) => {
    try {
        const users_id = req.user.id;
        // const role = req.user.role; // not needed since any auth user can insert based on original rules

        const { nama_kegiatan, tanggal_mulai, tanggal_selesai, penanggung_jawab, team_id, tempat_pelaksanaan, tahun, sasaran_peserta, total_peserta } = req.body;

        const parsedSasaran = parseInt(sasaran_peserta);
        const parsedTotal = parseInt(total_peserta);

        if (parsedTotal > parsedSasaran) {
            return res.status(400).json({
                message: "Total peserta tidak boleh melebihi sasaran peserta",
            });
        }

        const result = await pool.query(
            `INSERT INTO public.kegiatan
       (users_id, nama_kegiatan, tanggal_mulai, tanggal_selesai, penanggung_jawab, team_id, tempat_pelaksanaan, tahun, sasaran_peserta, total_peserta)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
       RETURNING *`,
            [users_id, nama_kegiatan, tanggal_mulai, tanggal_selesai, penanggung_jawab, team_id, tempat_pelaksanaan, tahun, parsedSasaran, parsedTotal],
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

export const getAllKegiatan = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const offset = (page - 1) * limit;

        const dataQuery = `
      SELECT 
        k.id,
        k.nama_kegiatan,
        k.tanggal_mulai,
        k.tanggal_selesai,
        k.penanggung_jawab,
        k.team_id,
        ut.nama_tim AS tim,
        k.tempat_pelaksanaan,
        k.tahun,
        k.sasaran_peserta,
        k.total_peserta,
        k.created_at,
        u.nama AS created_by
      FROM public.kegiatan k
      JOIN public.users u ON k.users_id = u.id
      LEFT JOIN public.user_team ut ON k.team_id = ut.id
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

export const getKegiatanById = async (req, res) => {
    try {
        const id = parseInt(req.params.id);

        if (isNaN(id)) {
            return res.status(400).json({ message: "ID tidak valid" });
        }

        const query = `
      SELECT 
        k.id,
        k.nama_kegiatan,
        k.tanggal_mulai,
        k.tanggal_selesai,
        k.penanggung_jawab,
        k.team_id,
        ut.nama_tim AS tim,
        k.tempat_pelaksanaan,
        k.tahun,
        k.sasaran_peserta,
        k.total_peserta,
        k.created_at,
        u.nama AS created_by
      FROM public.kegiatan k
      JOIN public.users u ON k.users_id = u.id
      LEFT JOIN public.user_team ut ON k.team_id = ut.id
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

export const searchKegiatanByName = async (req, res) => {
    try {
        const page = parseInt(req.query.page) || 1;
        const limit = parseInt(req.query.limit) || 10;
        const search = req.query.query || "";
        const offset = (page - 1) * limit;

        const dataQuery = `
      SELECT 
        k.id,
        k.nama_kegiatan,
        k.tanggal_mulai,
        k.tanggal_selesai,
        k.penanggung_jawab,
        k.team_id,
        ut.nama_tim AS tim,
        k.tempat_pelaksanaan,
        k.tahun,
        k.sasaran_peserta,
        k.total_peserta,
        k.created_at,
        u.nama AS created_by
      FROM public.kegiatan k
      JOIN public.users u ON k.users_id = u.id
      LEFT JOIN public.user_team ut ON k.team_id = ut.id
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

export const deleteKegiatanById = async (req, res) => {
    try {
        const id = parseInt(req.params.id);
        const users_id = req.user.id;
        const role = req.user.role;

        if (isNaN(id)) {
            return res.status(400).json({ message: "ID tidak valid" });
        }

        let deleteQuery;
        let queryParams;

        if (role === 'admin') {
            deleteQuery = `
              DELETE FROM public.kegiatan
              WHERE id = $1
              RETURNING *
            `;
            queryParams = [id];
        } else {
            deleteQuery = `
              DELETE FROM public.kegiatan
              WHERE id = $1 AND users_id = $2
              RETURNING *
            `;
            queryParams = [id, users_id];
        }

        const result = await pool.query(deleteQuery, queryParams);

        if (result.rows.length === 0) {
            return res.status(403).json({ message: "Kegiatan tidak ditemukan atau Anda tidak memiliki akses untuk menghapus kegiatan ini" });
        }

        res.json({ message: "Kegiatan berhasil dihapus" });
    } catch (err) {
        console.error("DELETE KEGIATAN ERROR:", err);
        res.status(500).json({ message: "Gagal menghapus kegiatan" });
    }
};

export const deleteAllKegiatan = async (req, res) => {
    try {
        const checkData = await pool.query("SELECT COUNT(*) FROM public.kegiatan");
        const totalData = parseInt(checkData.rows[0].count);

        if (totalData === 0) {
            return res.status(400).json({
                message: "Data sudah kosong",
            });
        }

        await pool.query("TRUNCATE TABLE public.kegiatan RESTART IDENTITY CASCADE");

        res.json({
            message: `data berhasil dihapus`,
        });
    } catch (err) {
        console.error("DELETE ALL KEGIATAN ERROR:", err);
        res.status(500).json({ message: "Gagal menghapus data" });
    }
};
