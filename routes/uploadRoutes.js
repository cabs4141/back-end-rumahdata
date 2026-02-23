import express from "express";
import fs from "fs";
import multer from "multer";
import { uploadPtk, uploadSekolah, uploadPeserta, uploadPpg, uploadKegiatan } from "../controllers/uploadController.js";
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

router.post("/upload/ptk", upload.single("file"), authenticateToken, uploadPtk);
router.post("/upload/sekolah", upload.single("file"), authenticateToken, uploadSekolah);
router.post("/upload/peserta", upload.single("file"), authenticateToken, uploadPeserta);
router.post("/upload/ppg", upload.single("file"), authenticateToken, uploadPpg);
router.post("/upload/kegiatan", upload.single("file"), authenticateToken, uploadKegiatan);

export default router;
