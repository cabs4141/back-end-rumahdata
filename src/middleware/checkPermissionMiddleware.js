import { pool } from "../config/db.js";

const checkPermission = () => {
  return async (req, res, next) => {
    try {
      if (!req.user) {
        return res.status(401).json({ message: "Unauthorized" });
      }

      if (req.user.role === "admin") {
        return next();
      }

      const result = await pool.query(
        "SELECT 1 FROM user_permissions WHERE user_id = $1 LIMIT 1",
        [req.user.id],
      );

      if (result.rowCount === 0) {
        return res.status(403).json({ message: "Access denied" });
      }

      return next();
    } catch (err) {
      console.error("ERROR:", err);
      return res.status(500).json({ message: "Server error" });
    }
  };
};

export default checkPermission;
