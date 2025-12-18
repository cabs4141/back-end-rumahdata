import express from "express";
import fs from "fs";
import multer from "multer";
import {
  insertExcelToDBPTK,
  insertExcelToDBDataSekolah,
  showAllData,
  verifyToken,
  showDataSekolahByNama,
} from "../controllers/excelController.js";
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

router.post("/excel/insertToPTK", upload.single("file"), insertExcelToDBPTK);
router.post(
  "/excel/insertToDataSekolah",
  upload.single("file"),
  insertExcelToDBDataSekolah
);
router.get("/excel/show-all", showAllData);
router.get(
  "/excel/show-data-sekolah-by-nama",
  verifyToken,
  showDataSekolahByNama
);
export default router;
