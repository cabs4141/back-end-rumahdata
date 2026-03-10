import express from "express";
import { register, login, createUser } from "./auth.controller.js";
import checkPermission from "../../middleware/checkPermissionMiddleware.js";
import {
  authenticateToken,
  adminOnly,
} from "../../middleware/authMiddleware.js";

const router = express.Router();

router.post("/register", register);
router.post("/login", login);
router.post(
  "/create-user",
  authenticateToken,
  checkPermission(),
  adminOnly,
  createUser,
);

export default router;
