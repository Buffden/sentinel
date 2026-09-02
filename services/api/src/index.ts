import http from 'node:http';
import express from 'express';
import cookieParser from 'cookie-parser';
import { authRouter } from './routes/auth.js';
import { alertsRouter } from './routes/alerts.js';
import { entitiesLiveRouter } from './routes/entitiesLive.js';
import { requireAuth } from './middleware/auth.js';
import { startAlertSink } from './sink/alertSink.js';
import { attachWebSocketServer } from './ws/wsServer.js';
import { config } from './config.js';

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

app.get('/healthz-auth', (_req, res) => {
	res.json({ ok: true, user_id: res.locals['userId'] as string });
});

app.use('/alerts', alertsRouter);
app.use('/entities/live', entitiesLiveRouter);

// Create HTTP server so we can intercept upgrade requests for WebSocket auth.
const server = http.createServer(app);

attachWebSocketServer(server);

startAlertSink().catch((err: unknown) => {
	console.error(
		JSON.stringify({ level: 'error', msg: 'alert sink failed to start', err: String(err) }),
	);
	process.exit(1);
});

server.listen(config.PORT, () => {
	console.log(JSON.stringify({ level: 'info', msg: 'API listening', port: config.PORT }));
});
