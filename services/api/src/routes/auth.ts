import { Router } from 'express';
import { OAuth2Client } from 'google-auth-library';
import jwt from 'jsonwebtoken';
import { randomUUID } from 'node:crypto';
import { pool } from '../db.js';
import { getDemoCount, MAX_DEMO_CONNECTIONS } from '../shared/demoSessions.js';

const router = Router();

const GOOGLE_CLIENT_ID = (process.env['GOOGLE_CLIENT_ID'] ?? '').trim();
const JWT_SECRET = process.env['JWT_SECRET'];
if (!JWT_SECRET) throw new Error('JWT_SECRET env var is required');
const jwtSecret: string = JWT_SECRET;
// 8-hour expiry: leaked token expires within the same working day.
const JWT_EXPIRES_IN = '8h';
const COOKIE_NAME = 'sentinel_jwt';

const oauthClient = new OAuth2Client(GOOGLE_CLIENT_ID);

// In-memory IP rate limit for POST /auth/demo: 1 token per IP per hour.
// Key: IP string. Value: timestamp (ms) of last token issued.
const demoRateLimit = new Map<string, number>();
const DEMO_RATE_LIMIT_WINDOW_MS = 60 * 60 * 1000; // 1 hour
const DEMO_JWT_EXPIRES_IN = '3m';

router.post('/google', async (req, res) => {
	const body = req.body as { id_token?: string };
	if (!body.id_token) {
		res.status(400).json({ error: 'id_token is required' });
		return;
	}

	let googleSub: string;
	let email: string;
	try {
		const ticket = await oauthClient.verifyIdToken({
			idToken: body.id_token,
			audience: GOOGLE_CLIENT_ID,
		});
		const payload = ticket.getPayload();
		if (!payload?.sub || !payload.email) {
			res.status(401).json({ error: 'Invalid Google token payload' });
			return;
		}
		googleSub = payload.sub;
		email = payload.email;
	} catch (err) {
		console.error(
			JSON.stringify({ level: 'warn', msg: 'Google token verification failed', err: String(err) }),
		);
		res.status(401).json({ error: 'Google token verification failed' });
		return;
	}

	// Upsert: first login creates the row; subsequent logins update last_login_at.
	// The new UUID is only used on insert — existing user_id is preserved on conflict.
	const now = new Date();
	let userId: string;
	try {
		const result = await pool.query<{ user_id: string }>(
			`INSERT INTO users (user_id, google_sub, email, last_login_at, created_at)
			 VALUES ($1, $2, $3, $4, $4)
			 ON CONFLICT (google_sub)
			 DO UPDATE SET email = EXCLUDED.email, last_login_at = EXCLUDED.last_login_at
			 RETURNING user_id`,
			[randomUUID(), googleSub, email, now],
		);
		userId = result.rows[0]!.user_id;
	} catch (err) {
		console.error(JSON.stringify({ level: 'error', msg: 'DB upsert failed', err: String(err) }));
		res.status(500).json({ error: 'Internal server error' });
		return;
	}

	const token = jwt.sign({ user_id: userId, email, role: 'operator' }, jwtSecret, {
		expiresIn: JWT_EXPIRES_IN,
	});

	// secure: false in development (HTTP). Must be true behind HTTPS in production.
	res.cookie(COOKIE_NAME, token, {
		httpOnly: true,
		secure: process.env['NODE_ENV'] === 'production',
		sameSite: 'strict',
	});

	console.log(
		JSON.stringify({ level: 'info', msg: 'operator authenticated', user_id: userId, email }),
	);
	res.json({ ok: true });
});

router.post('/demo', (req, res) => {
	const ip = req.ip ?? req.socket.remoteAddress ?? 'unknown';
	const now = Date.now();

	const lastIssued = demoRateLimit.get(ip);
	if (lastIssued !== undefined && now - lastIssued < DEMO_RATE_LIMIT_WINDOW_MS) {
		res.status(429).json({ error: 'Rate limit exceeded. Try again in an hour.' });
		return;
	}

	if (getDemoCount() >= MAX_DEMO_CONNECTIONS) {
		res.status(429).json({ error: 'Demo capacity reached. Try again later.' });
		return;
	}

	demoRateLimit.set(ip, now);

	const token = jwt.sign({ user_id: 'demo', email: 'demo', role: 'demo' }, jwtSecret, {
		expiresIn: DEMO_JWT_EXPIRES_IN,
	});

	res.cookie(COOKIE_NAME, token, {
		httpOnly: true,
		secure: process.env['NODE_ENV'] === 'production',
		sameSite: 'strict',
	});

	console.log(JSON.stringify({ level: 'info', msg: 'demo session issued', ip }));
	res.json({ ok: true });
});

export { router as authRouter };
