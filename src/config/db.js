import pkg from "pg";
import dotenv from "dotenv";

dotenv.config(); // Load .env

const { Pool } = pkg;

// Pool koneksi ke database
export const pool = new Pool({
  user: process.env.DB_USER,
  host: process.env.DB_HOST,
  database: process.env.DB_NAME,
  password: process.env.DB_PASSWORD,
  port: process.env.DB_PORT,
});

export async function connectDB() {
  try {
    const client = await pool.connect();
    console.log("Database connected");
    client.release();
  } catch (error) {
    console.log("Database connection error");
    process.exit(1);
  }
}
