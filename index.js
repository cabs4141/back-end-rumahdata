import express from "express";
import dotenv from "dotenv";
import cors from "cors";
import authRoutes from "./src/features/auth/auth.routes.js";
import userRoutes from "./src/features/users/users.routes.js";
import ptkRoutes from "./src/features/ptk/ptk.routes.js";
import sekolahRoutes from "./src/features/sekolah/sekolah.routes.js";
import pesertaRoutes from "./src/features/peserta/peserta.routes.js";
import kegiatanRoutes from "./src/features/kegiatan/kegiatan.routes.js";
import ppgRoutes from "./src/features/ppg/ppg.routes.js";
import uploadRoutes from "./src/features/upload/upload.routes.js";
import splitsdataRoutes from "./src/features/splitsdata/splitsdata.routes.js";
import userTeamRoutes from "./src/features/user_team/user_team.routes.js";
import deskripsiRoutes from "./src/features/deskripsi/deskripsi.routes.js";
import { connectDB } from "./src/config/db.js";

dotenv.config();
const app = express();
const PORT = process.env.PORT || 3000;
// middleware
app.use(cors());
app.use(express.json());
// routes
app.use("/api/auth", authRoutes);
app.use("/api", userRoutes);
app.use("/api", ptkRoutes);
app.use("/api", sekolahRoutes);
app.use("/api", pesertaRoutes);
app.use("/api", kegiatanRoutes);
app.use("/api", ppgRoutes);
app.use("/api", uploadRoutes);
app.use("/api/splitsdata", splitsdataRoutes);
app.use("/api", userTeamRoutes);
app.use("/api/deskripsi", deskripsiRoutes);
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
