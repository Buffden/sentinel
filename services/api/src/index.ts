import express from 'express';
import cookieParser from 'cookie-parser';
import { authRouter } from './routes/auth.js';
import { alertsRouter } from './routes/alerts.js';
import { requireAuth } from './middleware/auth.js';
import { startAlertSink } from './sink/alertSink.js';

const PORT = parseInt(process.env['PORT'] ?? '3000', 10);

const app = express();
app.use(express.json());
app.use(cookieParser());

// Unauthenticated routes

app.get('/healthz', (_req, res) => {
	res.json({ ok: true });
});

app.use('/auth', authRouter);

// Auth boundary
// Every route registered below this line requires a valid sentinel_jwt cookie.

app.use(requireAuth);

// Stub: verifies the auth boundary is enforced before any real routes exist.
// Returns 200 for a valid cookie, 401 for missing/invalid.
app.get('/healthz-auth', (_req, res) => {
	res.json({ ok: true, user_id: res.locals['userId'] as string });
});

app.use('/alerts', alertsRouter);

// Start alert sink consumer.
startAlertSink().catch((err: unknown) => {
	console.error(JSON.stringify({ level: 'error', msg: 'alert sink failed to start', err: String(err) }));
	process.exit(1);
});

app.listen(PORT, () => {
	console.log(JSON.stringify({ level: 'info', msg: 'API listening', port: PORT }));
});
