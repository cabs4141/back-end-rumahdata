import express from "express";
import fs from "fs";
import multer from "multer";
import { addToPtk, addToSekolah, addToPeserta, addtoPPG } from "../controllers/uploadController.js";
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

router.post("/upload/ptk", upload.single("file"), authenticateToken, addToPtk);
router.post("/upload/sekolah", upload.single("file"), authenticateToken, addToSekolah);
router.post("/upload/peserta", upload.single("file"), authenticateToken, addToPeserta);
router.post("/upload-ppg", authenticateToken, upload.single("file"), addtoPPG);
export default router;
