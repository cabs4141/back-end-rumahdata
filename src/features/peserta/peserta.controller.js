import { pool } from "../../config/db.js";

export const insertPeserta = async (req, res) => {
    try {
        const { kegiatan_id, nama, kabupaten, instansi, jabatan, alamat, jenjang, peran } = req.body;

        if (!kegiatan_id || !nama || !kabupaten || !instansi || !jabatan) {
            return res.status(400).json({ message: "Data wajib belum lengkap" });
        }

        const result = await pool.query(
            `INSERT INTO public.peserta (kegiatan_id, nama, kabupaten, instansi, jabatan, alamat, jenjang, peran)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) RETURNING *`,
            [kegiatan_id, nama, kabupaten, instansi, jabatan, alamat, jenjang, peran || 'peserta']
        );

        res.status(201).json({ message: "Peserta berhasil ditambahkan", data: result.rows[0] });
    } catch (error) {
        console.error("INSERT PESERTA ERROR:", error);
        res.status(500).json({ message: "Gagal menambahkan peserta" });
    }
};

export const getAllPeserta = async (req, res) => {
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

export const getPesertaDetail = async (req, res) => {
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

export const searchPeserta = async (req, res) => {
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

export const deletePeserta = async (req, res) => {
    try {
        const { id } = req.params;
        const users_id = req.user.id;
        const role = req.user.role;

        let deleteQuery;
        let queryParams;

        if (role === 'admin') {
            deleteQuery = "DELETE FROM public.peserta WHERE peserta_id = $1 RETURNING *";
            queryParams = [id];
        } else {
            deleteQuery = `
                DELETE FROM public.peserta 
                WHERE peserta_id = $1 AND kegiatan_id IN (
                    SELECT id FROM public.kegiatan WHERE users_id = $2
                ) 
                RETURNING *
            `;
            queryParams = [id, users_id];
        }

        const result = await pool.query(deleteQuery, queryParams);

        if (result.rows.length === 0) {
            return res.status(403).json({ message: "Peserta tidak ditemukan atau Anda tidak memiliki akses untuk menghapus peserta ini" });
        }

        res.json({ message: "Peserta berhasil dihapus" });
    } catch (err) {
        console.error("DELETE PESERTA ERROR:", err);
        res.status(500).json({ message: "Gagal menghapus peserta" });
    }
};

export const deleteAllPeserta = async (req, res) => {
    try {
        const kegiatanId = parseInt(req.params.kegiatanId);
        const users_id = req.user.id;
        const role = req.user.role;

        if (isNaN(kegiatanId)) {
            return res.status(400).json({ message: "ID kegiatan tidak valid" });
        }

        let deleteQuery;
        let queryParams;

        if (role === 'admin') {
            deleteQuery = "DELETE FROM public.peserta WHERE kegiatan_id = $1 RETURNING *";
            queryParams = [kegiatanId];
        } else {
            deleteQuery = `
                DELETE FROM public.peserta 
                WHERE kegiatan_id = $1 AND kegiatan_id IN (
                    SELECT id FROM public.kegiatan WHERE users_id = $2
                ) 
                RETURNING *
            `;
            queryParams = [kegiatanId, users_id];
        }

        const result = await pool.query(deleteQuery, queryParams);

        if (result.rows.length === 0) {
            // It could be 0 because there were no peserta, or because no permission.
            // But we will just return success if it executed successfully.
            // If they didn't have permission we could check first, but for now this acts safely.
            const checkPermsQuery = "SELECT id FROM public.kegiatan WHERE id = $1 AND users_id = $2";
            const checkPerms = await pool.query(checkPermsQuery, [kegiatanId, users_id]);
            if (role !== 'admin' && checkPerms.rows.length === 0) {
                return res.status(403).json({ message: "Kegiatan tidak ditemukan atau Anda tidak memiliki akses untuk menghapus peserta di kegiatan ini" });
            }
            return res.json({ message: "Data sudah kosong atau berhasil dihapus", data: [] });
        }

        res.json({
            message: `Data berhasil dihapus`,
            data: result.rows
        });
    } catch (err) {
        console.error("DELETE ALL PESERTA ERROR:", err);
        res.status(500).json({ message: "Gagal menghapus data" });
    }
};
