import express from "express";
import { getPTK, getSekolah } from "../controllers/csvController.js";
import { authenticateToken } from "../src/middleware/authMiddleware.js";

const router = express.Router();

router.get("/ptk", authenticateToken, getPTK);
router.get("/sekolah", authenticateToken, getSekolah);

export default router;
