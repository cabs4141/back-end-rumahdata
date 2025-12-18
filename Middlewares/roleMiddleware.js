export const superadminOnly = (req, res, next) => {
  if (req.user.role !== "superadmin") {
    return res.status(403).json({
      error: "Akses ditolak (Superadmin only)",
    });
  }
  next();
};
