import express from "express";
import dotenv from "dotenv";
import cors from "cors";

import csvRoutes from "./routes/csvRoutes.js";
import excelRoutes from "./routes/excelRoutes.js";
import authRoutes from "./routes/authRoutes.js";
import adminRoutes from "./routes/adminRoutes.js";
import userRoutes from "./routes/userRoutes.js";
import kegiatanRoutes from "./routes/kegiatanRoutes.js";

import { connectDB } from "./src/db.js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// middleware
app.use(cors());
app.use(express.json());

// routes
app.use("/api", csvRoutes);
app.use("/api", excelRoutes);
app.use("/api/auth", authRoutes);
app.use("/api/admin", adminRoutes);
app.use("/api", userRoutes);
app.use("/api", kegiatanRoutes);

// root endpoint
app.get("/", (req, res) => {
  res.send("Server berjalan...");
});

// start server
async function startServer() {
  try {
    await connectDB();
    app.listen(PORT, () => {
      console.log(`Server running on port ${PORT}...`);
    });
  } catch (err) {
    console.error("DB gagal connect, server tidak dijalankan");
    process.exit(1);
  }
}

startServer();
