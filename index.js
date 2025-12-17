import express from "express";
import dotenv from "dotenv";
import cors from "cors";

import authRoutes from "./routes/authRoutes.js";
import csvRoutes from "./routes/csvRoutes.js";
import adminRoutes from './routes/adminRoutes.js';
import { connectDB } from "./src/db.js";

dotenv.config();

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

app.use("/api", csvRoutes);
app.use("/api/auth", authRoutes);
app.use('/api/admin', adminRoutes);

app.get("/", (req, res) => {
  res.send("Server berjalan...");
});

async function startServer() {
  try {
    await connectDB(); 
    app.listen(PORT, () =>
      console.log(`Server running on port ${PORT}...`)
    );
  } catch (err) {
    console.error(" DB gagal connect, server tidak dijalankan");
    process.exit(1);
  }
}

startServer();
