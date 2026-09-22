import { Router } from 'express';
import { asyncHandler } from '../shared/asyncHandler.js';
import { scanLiveEntities } from '../shared/liveEntities.js';

const router = Router();

router.get(
	'/',
	asyncHandler(async (req, res) => {
		const bboxParam = req.query['bbox'] as string | undefined;
		if (!bboxParam) {
			res.status(400).json({ error: 'bbox query parameter is required' });
			return;
		}

		const parts = bboxParam.split(',').map(Number);
		if (parts.length !== 4 || parts.some((n) => !isFinite(n))) {
			res.status(400).json({ error: 'bbox must be minLat,minLon,maxLat,maxLon' });
			return;
		}
		const [minLat, minLon, maxLat, maxLon] = parts as [number, number, number, number];

		const entities = await scanLiveEntities(
			(e) => e.lat >= minLat && e.lat <= maxLat && e.lon >= minLon && e.lon <= maxLon,
		);

		res.json(entities);
	}),
);

export { router as entitiesLiveRouter };
