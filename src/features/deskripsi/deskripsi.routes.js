import express from "express";
import {
  getDeskripsi,
  updateDeskripsi,
} from "./deskripsi.controller.js";

const router = express.Router();

router.get("/", getDeskripsi);
router.put("/", updateDeskripsi);

export default router;
