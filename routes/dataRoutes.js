import express from "express";
import { getPTK, getSekolah, deleteAllPtk, deleteAllSekolah, addKegiatan, searchSekolah, searchPTK, getSekolahDetail } from "../controllers/dataController.js";
import { authenticateToken } from "../src/middleware/authMiddleware.js";

const router = express.Router();

//ptk
router.get("/ptk", authenticateToken, getPTK);
router.get("/ptk/search", authenticateToken, searchPTK);
router.delete("/ptk", authenticateToken, deleteAllPtk);
//sekolah
router.get("/sekolah", authenticateToken, getSekolah);
router.get("/sekolah/search", authenticateToken, searchSekolah);
router.get("/sekolah/:sekolah_id/ptk", getSekolahDetail);
router.delete("/sekolah", authenticateToken, deleteAllSekolah);

// router.get("/sekolah/search", authenticateToken, getSekolahByName);
//kegiatan
router.post("/kegiatan", authenticateToken, addKegiatan);

export default router;
