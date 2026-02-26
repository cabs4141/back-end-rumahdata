import express from "express";
import { register, login, createUser } from "../controllers/authController.js";
import checkPermission from "../src/middleware/checkPermissionMiddleware.js";
import {
  authenticateToken,
  superAdminOnly,
} from "../src/middleware/authMiddleware.js";

const router = express.Router();

router.post("/register", register);
router.post("/login", login);
router.post(
  "/create-user",
  authenticateToken,
  checkPermission(),
  superAdminOnly,
  createUser,
);

export default router;
