import jwt from "jsonwebtoken";
import dotenv from "dotenv";
dotenv.config();

const authenticateToken = (req, res, next) => {
  const authHeader = req.headers["authorization"];

  // Format header bearer token"
  const token = authHeader && authHeader.split(" ")[1];

  if (!token) {
    return res.status(401).json({ message: "Unauthorized" });
  }

  // Cek apakah ada tanda kutip ganda yang terbawa (Common Bug)
  const cleanToken = token.replace(/"/g, "");

  jwt.verify(cleanToken, process.env.JWT_SECRET, (err, user) => {
    if (err) {
      console.error("JWT Error:", err.message);

      return res.status(403).json({
        message: "Token tidak valid atau kadaluarsa",
        detail: err.message,
      });
    }

    // Jika sukses, simpan data user ke req.user agar bisa dipakai di controller
    req.user = user;
    next();
  });
};

export { authenticateToken };
