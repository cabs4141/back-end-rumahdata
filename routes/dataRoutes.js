import express from "express";
import {
  getPTK,
  getSekolah,
  deleteAllPtk,
  deleteAllSekolah,
  searchSekolah,
  searchPTK,
  getSekolahDetail,
  getPesertaDetail,
  searchPeserta,
  deletePeserta,
  deleteAllPeserta,
  getAllPPG,
  searchPPG,
  deleteAllPPG,
  insertKegiatan,
  deleteKegiatanById,
  deleteAllKegiatan,
  getAllKegiatan,
  getKegiatanById,
  searchKegiatanByName,
  getAllPeserta,
} from "../controllers/dataController.js";
import { authenticateToken } from "../src/middleware/authMiddleware.js";

const router = express.Router();

//PTK
router.get("/ptk", authenticateToken, getPTK);
router.get("/ptk/search", authenticateToken, searchPTK);
router.delete("/ptk", authenticateToken, deleteAllPtk);
//sekolah
router.get("/sekolah", authenticateToken, getSekolah);
router.get("/sekolah/search", authenticateToken, searchSekolah);
router.get("/sekolah/:sekolah_id/ptk", authenticateToken, getSekolahDetail);
router.delete("/sekolah", authenticateToken, deleteAllSekolah);
//kegiatan
router.get("/kegiatan", authenticateToken, getAllKegiatan);
router.get("/kegiatan/search", authenticateToken, searchKegiatanByName);
router.get("/kegiatan/:id", authenticateToken, getKegiatanById);
router.post("/kegiatan", authenticateToken, insertKegiatan);
router.delete("/kegiatan/:id", authenticateToken, deleteKegiatanById);
router.delete("/kegiatan", authenticateToken, deleteAllKegiatan);
//peserta
router.get("/peserta", authenticateToken, getAllPeserta);
router.get("/peserta/search", authenticateToken, searchPeserta);
router.get("/peserta/:id", authenticateToken, getPesertaDetail);
router.delete("/peserta", authenticateToken, deleteAllPeserta);
router.delete("/peserta/:id", authenticateToken, deletePeserta);
//PPG
router.get("/ppg", authenticateToken, getAllPPG);
router.get("/ppg/search", authenticateToken, searchPPG);
router.delete("/ppg", authenticateToken, deleteAllPPG);

export default router;
