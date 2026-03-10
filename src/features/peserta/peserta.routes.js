import express from "express";
import { getAllPeserta, getPesertaDetail, searchPeserta, deletePeserta, deleteAllPeserta, insertPeserta } from "./peserta.controller.js";
import { authenticateToken, adminOnly } from "../../middleware/authMiddleware.js";

const router = express.Router();

router.get("/peserta", authenticateToken, getAllPeserta);
router.get("/peserta/search", authenticateToken, searchPeserta);
router.post("/peserta", authenticateToken, insertPeserta);
router.get("/peserta/:id", authenticateToken, getPesertaDetail);
router.delete("/peserta/kegiatan/:kegiatanId", authenticateToken, deleteAllPeserta);
router.delete("/peserta/:id", authenticateToken, deletePeserta);

export default router;
