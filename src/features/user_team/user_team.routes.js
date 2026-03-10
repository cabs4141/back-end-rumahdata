import express from "express";
import { getAllUserTeam, insertUserTeam, deleteUserTeam, updateUserTeam } from "./user_team.controller.js";
import { authenticateToken, adminOnly } from "../../middleware/authMiddleware.js";

const router = express.Router();

router.get("/user-team", authenticateToken, getAllUserTeam);
router.post("/user-team", authenticateToken, adminOnly, insertUserTeam);
router.put("/user-team/:id", authenticateToken, adminOnly, updateUserTeam);
router.delete("/user-team/:id", authenticateToken, adminOnly, deleteUserTeam);

export default router;
