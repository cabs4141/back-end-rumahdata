import express from "express";
import { getDeskripsi, updateDeskripsi } from "./deskripsi.controller.js";

const router = express.Router();

router.get("/deskripsi", getDeskripsi);
router.put("/deskripsi", updateDeskripsi);

export default router;
