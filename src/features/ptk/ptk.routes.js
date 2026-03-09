import express from "express";
import { getPTK, getPtkStatistik, searchPTK, deleteAllPtk } from "./ptk.controller.js";
import { authenticateToken, adminOnly } from "../../middleware/authMiddleware.js";

const router = express.Router();

router.get("/ptk", authenticateToken, getPTK);
router.get("/ptk/statistik", authenticateToken, getPtkStatistik);
router.get("/ptk/search", authenticateToken, searchPTK);
router.delete("/ptk", authenticateToken, adminOnly, deleteAllPtk);

export default router;
