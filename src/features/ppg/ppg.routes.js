import express from "express";
import { getAllPPG, searchPPG, deleteAllPPG } from "./ppg.controller.js";
import { authenticateToken, adminOnly } from "../../middleware/authMiddleware.js";

const router = express.Router();

router.get("/ppg", authenticateToken, getAllPPG);
router.get("/ppg/search", authenticateToken, searchPPG);
router.delete("/ppg", authenticateToken, adminOnly, deleteAllPPG);

export default router;
