import { Router } from 'express';
import { pool } from '../db.js';

const router = Router();

router.get('/', async (_req, res) => {
	const result = await pool.query(
		`SELECT alert_id, entity_id, entity_type, alert_type, priority, status,
						payload, detected_at, updated_at, acknowledged_at, resolved_at
		 FROM alerts
		 WHERE status IN ('NEW', 'ACKNOWLEDGED')
		 ORDER BY detected_at DESC`,
	);
	res.json(result.rows);
});

export { router as alertsRouter };
