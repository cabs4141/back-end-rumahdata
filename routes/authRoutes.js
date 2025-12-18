import express from "express";
import {
  register,
  login,
  logout,
  approveUser,
} from "../controllers/authController.js";

import { authMiddleware } from "../Middlewares/authMiddleware.js";
import { superadminOnly } from "../Middlewares/roleMiddleware.js";

const router = express.Router();

// Public
router.post("/register", register);
router.post("/login", login);

// Protected
router.post("/logout", authMiddleware, logout);

// 🔐 SUPERADMIN ONLY
router.post(
  "/approve",
  authMiddleware,
  superadminOnly,
  approveUser
);

export default router;
