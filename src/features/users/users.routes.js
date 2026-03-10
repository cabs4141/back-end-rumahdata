import express from "express";
import { deleteUserById, getUserById, getUsersList, updateUser } from "./users.controller.js";
import { authenticateToken, adminOnly } from "../../middleware/authMiddleware.js";

const router = express.Router();

router.get("/users", authenticateToken, getUsersList);
router.get("/user/:id", authenticateToken, getUserById);
router.post("/user/:id", authenticateToken, adminOnly, updateUser);
router.delete("/user/:id", authenticateToken, adminOnly, deleteUserById);

export default router;
