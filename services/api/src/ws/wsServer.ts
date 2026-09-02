import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage } from 'node:http';
import type { Server } from 'node:http';
import { parseCookie } from 'cookie';
import jwt from 'jsonwebtoken';
import { Redis } from 'ioredis';
import type { SentinelJwtPayload } from '../middleware/auth.js';
import { incrementDemoCount, decrementDemoCount } from '../shared/demoSessions.js';
import { config } from '../config.js';

interface BBox {
	minLat: number;
	minLon: number;
	maxLat: number;
	maxLon: number;
}

interface SubscribeMessage {
	type: 'subscribe';
	bbox: [number, number, number, number]; // minLat, minLon, maxLat, maxLon
}

// Per-connection bbox. Null until client sends a subscribe message.
const connectionBBox = new Map<WebSocket, BBox | null>();

function verifyToken(req: IncomingMessage): SentinelJwtPayload | null {
	const cookieHeader = req.headers['cookie'] ?? '';
	const cookies = parseCookie(cookieHeader);
	const token = cookies['sentinel_jwt'];
	if (!token) return null;
	try {
		return jwt.verify(token, config.JWT_SECRET) as unknown as SentinelJwtPayload;
	} catch {
		return null;
	}
}

function isWithinBBox(bbox: BBox, lat: number, lon: number): boolean {
	return lat >= bbox.minLat && lat <= bbox.maxLat && lon >= bbox.minLon && lon <= bbox.maxLon;
}

export function attachWebSocketServer(server: Server): void {
	const wss = new WebSocketServer({ noServer: true });

	// Dedicated subscriber connection — cannot issue commands on a subscribed connection.
	const redisSub = new Redis(config.REDIS_URL);

	redisSub.subscribe(config.POSITION_UPDATES_CHANNEL, config.ALERT_EVENTS_CHANNEL, (err) => {
		if (err) {
			console.error(
				JSON.stringify({ level: 'error', msg: 'redis subscribe failed', err: String(err) }),
			);
		} else {
			console.log(
				JSON.stringify({
					level: 'info',
					msg: 'ws redis subscriber ready',
					channels: [config.POSITION_UPDATES_CHANNEL, config.ALERT_EVENTS_CHANNEL],
				}),
			);
		}
	});

	redisSub.on('message', (channel, message) => {
		if (channel === config.POSITION_UPDATES_CHANNEL) {
			let parsed: { lat?: unknown; lon?: unknown } = {};
			try {
				parsed = JSON.parse(message) as { lat?: unknown; lon?: unknown };
			} catch {
				return;
			}
			const lat =
				typeof parsed.lat === 'number' ? parsed.lat : parseFloat(String(parsed.lat ?? ''));
			const lon =
				typeof parsed.lon === 'number' ? parsed.lon : parseFloat(String(parsed.lon ?? ''));
			if (!isFinite(lat) || !isFinite(lon)) return;

			for (const [ws, bbox] of connectionBBox) {
				if (ws.readyState !== WebSocket.OPEN) continue;
				if (bbox && !isWithinBBox(bbox, lat, lon)) continue;
				ws.send(JSON.stringify({ channel: config.POSITION_UPDATES_CHANNEL, data: parsed }));
			}
		} else if (channel === config.ALERT_EVENTS_CHANNEL) {
			// Alert events go to all connected clients — no bbox filter.
			for (const [ws] of connectionBBox) {
				if (ws.readyState !== WebSocket.OPEN) continue;
				ws.send(
					JSON.stringify({ channel: config.ALERT_EVENTS_CHANNEL, data: JSON.parse(message) }),
				);
			}
		}
	});

	// Intercept HTTP upgrade requests — validate JWT before completing handshake.
	server.on('upgrade', (req, socket, head) => {
		const payload = verifyToken(req);
		if (!payload) {
			socket.write('HTTP/1.1 401 Unauthorized\r\n\r\n');
			socket.destroy();
			return;
		}

		wss.handleUpgrade(req, socket, head, (ws) => {
			wss.emit('connection', ws, req, payload);
		});
	});

	wss.on('connection', (ws: WebSocket, _req: IncomingMessage, payload: SentinelJwtPayload) => {
		connectionBBox.set(ws, null);

		// Demo session: track active count and schedule close at JWT expiry.
		let demoExpiryTimer: ReturnType<typeof setTimeout> | null = null;
		if (payload.role === 'demo' && payload.exp !== undefined) {
			incrementDemoCount();
			const msUntilExpiry = payload.exp * 1000 - Date.now();
			// Guard against already-expired tokens — they would have been rejected at upgrade,
			// but a negative delay would fire immediately and confuse the operator.
			const delay = Math.max(msUntilExpiry, 0);
			demoExpiryTimer = setTimeout(() => {
				ws.close(4401, 'demo session expired');
			}, delay);
		}

		console.log(
			JSON.stringify({
				level: 'info',
				msg: 'ws client connected',
				user_id: payload.user_id,
				role: payload.role,
				total: connectionBBox.size,
			}),
		);

		ws.on('message', (data) => {
			let msg: SubscribeMessage;
			try {
				msg = JSON.parse(data.toString()) as SubscribeMessage;
			} catch {
				return;
			}
			if (msg.type === 'subscribe' && Array.isArray(msg.bbox) && msg.bbox.length === 4) {
				const [minLat, minLon, maxLat, maxLon] = msg.bbox;
				connectionBBox.set(ws, { minLat, minLon, maxLat, maxLon });
				console.log(
					JSON.stringify({
						level: 'info',
						msg: 'ws bbox updated',
						user_id: payload.user_id,
						bbox: msg.bbox,
					}),
				);
			}
		});

		ws.on('close', () => {
			connectionBBox.delete(ws);
			if (payload.role === 'demo') {
				decrementDemoCount();
				if (demoExpiryTimer !== null) clearTimeout(demoExpiryTimer);
			}
			console.log(
				JSON.stringify({
					level: 'info',
					msg: 'ws client disconnected',
					user_id: payload.user_id,
					role: payload.role,
					total: connectionBBox.size,
				}),
			);
		});

		ws.on('error', (err) => {
			console.error(
				JSON.stringify({
					level: 'error',
					msg: 'ws client error',
					user_id: payload.user_id,
					err: String(err),
				}),
			);
		});
	});
}
