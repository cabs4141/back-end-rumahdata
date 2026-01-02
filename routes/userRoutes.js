import express from "express";
import { getUsersList } from "../controllers/getUsersController.js";
import { verifyToken } from "../controllers/excelController.js";

const router = express.Router();

router.get(
  "/users",
  verifyToken,
  (req, res, next) => {
    if (req.user.role !== "super_admin") {
      return res.status(403).json({
        message: "Akses hanya untuk super admin",
      });
    }
    next();
  },
  getUsersList
);

export default router;
