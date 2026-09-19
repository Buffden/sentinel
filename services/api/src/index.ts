import http from 'node:http';
import express, { type ErrorRequestHandler } from 'express';
import cookieParser from 'cookie-parser';
import { authRouter } from './routes/auth.js';
import { alertsRouter } from './routes/alerts.js';
import { entitiesLiveRouter } from './routes/entitiesLive.js';
import { workspaceRouter } from './routes/workspace.js';
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
app.use('/users/me/workspace', workspaceRouter);

// Express 4 does not forward a rejected promise from an async route handler
// to error-handling middleware on its own -- an uncaught rejection here
// (e.g. an unexpected Postgres error) would otherwise leave the request
// hanging with no response at all, rather than failing cleanly. Must be
// registered after every route; Express identifies this as error-handling
// middleware by its 4-argument signature.
const handleUnhandledRouteError: ErrorRequestHandler = (err, _req, res, next) => {
	if (res.headersSent) {
		next(err);
		return;
	}
	console.error(
		JSON.stringify({ level: 'error', msg: 'unhandled request error', err: String(err) }),
	);
	res.status(500).json({ error: 'internal error' });
};
app.use(handleUnhandledRouteError);

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
