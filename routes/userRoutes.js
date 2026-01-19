import express from "express";
import { deleteUserById, getUserById, getUsersList } from "../controllers/usersController.js";
import { authenticateToken, superAdminOnly } from "../src/middleware/authMiddleware.js";
import { approveUser } from "../controllers/usersController.js";

const router = express.Router();

router.get("/users", authenticateToken, getUsersList);
router.get("/user/:id", authenticateToken, getUserById);
router.post("/user", authenticateToken, superAdminOnly, approveUser);
router.delete("/user/:id", authenticateToken, superAdminOnly, deleteUserById);

export default router;
