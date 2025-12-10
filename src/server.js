import express from "express";
import cors from "cors";
import session from "express-session";
import authRoutes from "./routes/auth.js";

const app = express();

app.use(
  cors({
    origin: "http://localhost:5173",
    credentials: true,
  })
);

app.use(express.json());

app.use(
  session({
    secret: "secret-key",
    resave: false,
    saveUninitialized: false,
    cookie: { secure: false }, // kalau HTTPS harus true
  })
);

// Routes
app.use("/api/auth", authRoutes);
