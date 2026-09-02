import { Router } from 'express';
import { OAuth2Client } from 'google-auth-library';
import jwt from 'jsonwebtoken';
import { randomUUID } from 'node:crypto';
import { pool } from '../db.js';
import { getDemoCount } from '../shared/demoSessions.js';
import { config } from '../config.js';

const router = Router();

const oauthClient = new OAuth2Client(config.GOOGLE_CLIENT_ID);

// In-memory IP rate limit for POST /auth/demo: 1 token per IP per DEMO_RATE_LIMIT_WINDOW_MS.
// Key: IP string. Value: timestamp (ms) of last token issued.
const demoRateLimit = new Map<string, number>();

router.post('/google', async (req, res) => {
	const body = req.body as { id_token?: string };
	if (!body.id_token) {
		res.status(400).json({ error: 'id_token is required' });
		return;
	}

	let googleSub: string;
	let email: string;
	let name: string;
	try {
		const ticket = await oauthClient.verifyIdToken({
			idToken: body.id_token,
			audience: config.GOOGLE_CLIENT_ID,
		});
		const payload = ticket.getPayload();
		if (!payload?.sub || !payload.email) {
			res.status(401).json({ error: 'Invalid Google token payload' });
			return;
		}
		googleSub = payload.sub;
		email = payload.email;
		name = payload.name ?? payload.email;
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

	const token = jwt.sign({ user_id: userId, email, name, role: 'operator' }, config.JWT_SECRET, {
		expiresIn: config.JWT_EXPIRES_IN,
	});

	// secure: false in development (HTTP). Must be true behind HTTPS in production.
	res.cookie(config.COOKIE_NAME, token, {
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
	if (lastIssued !== undefined && now - lastIssued < config.DEMO_RATE_LIMIT_WINDOW_MS) {
		res.status(429).json({ error: 'Rate limit exceeded. Try again in an hour.' });
		return;
	}

	if (getDemoCount() >= config.MAX_DEMO_CONNECTIONS) {
		res.status(429).json({ error: 'Demo capacity reached. Try again later.' });
		return;
	}

	demoRateLimit.set(ip, now);

	const token = jwt.sign({ user_id: 'demo', email: 'demo', role: 'demo' }, config.JWT_SECRET, {
		expiresIn: config.DEMO_JWT_EXPIRES_IN,
	});

	res.cookie(config.COOKIE_NAME, token, {
		httpOnly: true,
		secure: process.env['NODE_ENV'] === 'production',
		sameSite: 'strict',
	});

	console.log(JSON.stringify({ level: 'info', msg: 'demo session issued', ip }));
	res.json({ ok: true });
});

router.get('/me', (req, res) => {
	const token = req.cookies?.[config.COOKIE_NAME] as string | undefined;
	if (!token) {
		res.status(401).json({ error: 'Not authenticated' });
		return;
	}
	try {
		const payload = jwt.verify(token, config.JWT_SECRET) as {
			user_id?: string;
			email?: string;
			name?: string;
			role?: string;
		};
		res.json({
			user_id: payload.user_id,
			email: payload.email,
			name: payload.name,
			role: payload.role,
		});
	} catch {
		res.status(401).json({ error: 'Invalid or expired token' });
	}
});

router.post('/logout', (req, res) => {
	const token = req.cookies?.[config.COOKIE_NAME] as string | undefined;

	if (token) {
		try {
			const payload = jwt.verify(token, config.JWT_SECRET) as {
				user_id?: string;
				email?: string;
				role?: string;
			};
			console.log(
				JSON.stringify({
					level: 'info',
					msg: 'logout',
					role: payload.role ?? 'unknown',
					user_id: payload.user_id ?? 'unknown',
				}),
			);
		} catch {
			// Token expired or invalid — still clear the cookie.
		}
	}

	res.clearCookie(config.COOKIE_NAME, {
		httpOnly: true,
		secure: process.env['NODE_ENV'] === 'production',
		sameSite: 'strict',
	});
	res.json({ ok: true });
});

export { router as authRouter };
