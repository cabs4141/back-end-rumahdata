import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import dataRoutes from "./routes/dataRoutes.js";
import uploadRoutes from "./routes/uploadRoutes.js";
import authRoutes from "./routes/authRoutes.js";
import splitsdataRoutes from "./routes/splitsdataRoutes.js";

import userRoutes from "./routes/userRoutes.js";
import { connectDB } from "./src/db.js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

// middleware
app.use(cors());
app.use(express.json());

// routes
app.use("/api", dataRoutes);
app.use("/api", uploadRoutes);
app.use("/api/auth", authRoutes);
app.use("/api/splitsdata", splitsdataRoutes);
app.use("/api", userRoutes);

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
