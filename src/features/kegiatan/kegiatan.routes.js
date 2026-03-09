import express from "express";
import { insertKegiatan, getAllKegiatan, getKegiatanById, searchKegiatanByName, deleteKegiatanById, deleteAllKegiatan } from "./kegiatan.controller.js";
import { authenticateToken, adminOnly } from "../../middleware/authMiddleware.js";

const router = express.Router();

router.get("/kegiatan", authenticateToken, getAllKegiatan);
router.get("/kegiatan/search", authenticateToken, searchKegiatanByName);
router.get("/kegiatan/:id", authenticateToken, getKegiatanById);
router.post("/kegiatan", authenticateToken, insertKegiatan);
router.delete("/kegiatan/:id", authenticateToken, deleteKegiatanById);
router.delete("/kegiatan", authenticateToken, adminOnly, deleteAllKegiatan);

export default router;
