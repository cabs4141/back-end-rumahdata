import express from 'express';
import { approveUser } from '../controllers/adminController.js';
const router = express.Router();

// Endpoint hanya untuk super admin
router.post('/approve-user', approveUser);

export default router;