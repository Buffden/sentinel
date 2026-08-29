import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage } from 'node:http';
import type { Server } from 'node:http';
import { parseCookie } from 'cookie';
import jwt from 'jsonwebtoken';
import { Redis } from 'ioredis';
import type { SentinelJwtPayload } from '../middleware/auth.js';

const JWT_SECRET = process.env['JWT_SECRET'];
if (!JWT_SECRET) throw new Error('JWT_SECRET env var is required');
const jwtSecret: string = JWT_SECRET;

const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://localhost:6379';

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
		return jwt.verify(token, jwtSecret) as unknown as SentinelJwtPayload;
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
	const redisSub = new Redis(REDIS_URL);

	redisSub.subscribe('position-updates', 'alert-events', (err) => {
		if (err) {
			console.error(
				JSON.stringify({ level: 'error', msg: 'redis subscribe failed', err: String(err) }),
			);
		} else {
			console.log(
				JSON.stringify({
					level: 'info',
					msg: 'ws redis subscriber ready',
					channels: ['position-updates', 'alert-events'],
				}),
			);
		}
	});

	redisSub.on('message', (channel, message) => {
		if (channel === 'position-updates') {
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
				ws.send(JSON.stringify({ channel: 'position-updates', data: parsed }));
			}
		} else if (channel === 'alert-events') {
			// Alert events go to all connected clients — no bbox filter.
			for (const [ws] of connectionBBox) {
				if (ws.readyState !== WebSocket.OPEN) continue;
				ws.send(JSON.stringify({ channel: 'alert-events', data: JSON.parse(message) }));
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
		console.log(
			JSON.stringify({
				level: 'info',
				msg: 'ws client connected',
				user_id: payload.user_id,
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
			console.log(
				JSON.stringify({
					level: 'info',
					msg: 'ws client disconnected',
					user_id: payload.user_id,
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
