import express from "express";
import fs from "fs";
import multer from "multer";
import {
  insertKegiatan,
  deleteKegiatanById,
  deleteAllKegiatan,
  getAllKegiatan,
  getKegiatanById,
  searchKegiatanByName,
  uploadKegiatan,
} from "../controllers/KegiatanController.js";
import { authenticateToken } from "../src/middleware/authMiddleware.js";

const router = express.Router();

if (!fs.existsSync("uploads")) {
  fs.mkdirSync("uploads", { recursive: true });
}
const storage = multer.diskStorage({
  destination: "uploads/",
  filename: (_, file, cb) => {
    cb(null, Date.now() + "-" + file.originalname);
  },
});

const maxMB = Number(process.env.UPLOAD_MAX_MB || 200);

const upload = multer({
  storage,
  limits: {
    fileSize: maxMB * 1024 * 1024, // 200 MB
  },
});

// Add Kegiatan
router.post("/addKegiatan", authenticateToken, insertKegiatan);
// Hapus Kegiatan
router.delete("/deleteKegiatan/:id", authenticateToken, deleteKegiatanById);
router.delete("/deleteAllKegiatan", authenticateToken, deleteAllKegiatan);

// Get Kegiatan
router.get("/getAllKegiatan", authenticateToken, getAllKegiatan);
router.get("/getKegiatanById/:id", authenticateToken, getKegiatanById);

// Search Kegiatan
router.get("/searchKegiatanByName/", authenticateToken, searchKegiatanByName);

// Upload
router.post(
  "/upload-kegiatan",
  authenticateToken,
  upload.single("file"),
  uploadKegiatan,
);

export default router;
