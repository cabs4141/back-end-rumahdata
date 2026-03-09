import express from "express";
import { getSekolah, searchSekolah, getSekolahDetail, deleteAllSekolah } from "./sekolah.controller.js";
import { authenticateToken, adminOnly } from "../../middleware/authMiddleware.js";

const router = express.Router();

router.get("/sekolah", authenticateToken, getSekolah);
router.get("/sekolah/search", authenticateToken, searchSekolah);
router.get("/sekolah/:sekolah_id/ptk", authenticateToken, getSekolahDetail);
router.delete("/sekolah", authenticateToken, adminOnly, deleteAllSekolah);

export default router;
