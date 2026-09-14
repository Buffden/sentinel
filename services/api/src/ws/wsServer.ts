import { WebSocketServer, WebSocket } from 'ws';
import type { IncomingMessage } from 'node:http';
import type { Server } from 'node:http';
import { parseCookie } from 'cookie';
import jwt from 'jsonwebtoken';
import { Redis } from 'ioredis';
import type { SentinelJwtPayload } from '../middleware/auth.js';
import { incrementDemoCount, decrementDemoCount } from '../shared/demoSessions.js';
import { pool } from '../db.js';
import {
	matchesScope,
	type AlertForScopeCheck,
	type ScopeFilter,
} from '../shared/alertScopeFilter.js';
import type { GeoBounds } from '../shared/regions.js';
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

interface WorkspaceScopeRow {
	geo_region: { bounds: GeoBounds };
	entity_types: string[];
	alert_types: string[];
}

// One record per open WebSocket. positionBBox drives position-updates
// filtering (unchanged from before CP3). operatorScope/scopeLoaded are new:
// loaded once from user_workspaces at connection open, for role === 'operator'
// only -- see the ws-alert-scope-filtering concept doc for why this replaced
// a bare connectionBBox map.
interface ConnectionState {
	role: 'operator' | 'demo';
	userId: string;
	positionBBox: BBox | null;
	operatorScope: ScopeFilter | null;
	scopeLoaded: boolean;
}

const connections = new Map<WebSocket, ConnectionState>();

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

function bboxToGeoBounds(bbox: BBox): GeoBounds {
	return { min_lat: bbox.minLat, max_lat: bbox.maxLat, min_lon: bbox.minLon, max_lon: bbox.maxLon };
}

// Single lookup at connection open -- see ADR-012 and the ws-alert-scope-filtering
// concept doc for why this is not re-queried per message. Returns null both
// for "no saved workspace" and (via the caller's catch) "lookup failed" --
// either way, the connection stays fail-closed for alert delivery.
async function loadOperatorScope(userId: string): Promise<ScopeFilter | null> {
	const result = await pool.query<{ scope: WorkspaceScopeRow }>(
		'SELECT scope FROM user_workspaces WHERE user_id = $1',
		[userId],
	);
	if (result.rows.length === 0) return null;
	const scope = result.rows[0]!.scope;
	return {
		bounds: scope.geo_region.bounds,
		entity_types: scope.entity_types,
		alert_types: scope.alert_types,
	};
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

			for (const [ws, state] of connections) {
				if (ws.readyState !== WebSocket.OPEN) continue;
				if (state.positionBBox && !isWithinBBox(state.positionBBox, lat, lon)) continue;
				ws.send(JSON.stringify({ channel: config.POSITION_UPDATES_CHANNEL, data: parsed }));
			}
		} else if (channel === config.ALERT_EVENTS_CHANNEL) {
			let alert: (AlertForScopeCheck & Record<string, unknown>) | null;
			try {
				alert = JSON.parse(message) as AlertForScopeCheck & Record<string, unknown>;
			} catch {
				return;
			}
			const envelope = JSON.stringify({ channel: config.ALERT_EVENTS_CHANNEL, data: alert });

			for (const [ws, state] of connections) {
				if (ws.readyState !== WebSocket.OPEN) continue;

				if (state.role === 'operator') {
					// Fail closed: no saved workspace, or the async load hasn't
					// resolved yet, both mean "deliver nothing" -- never a guess.
					if (!state.scopeLoaded || !state.operatorScope) continue;
					if (!matchesScope(alert, state.operatorScope)) continue;
				} else if (state.positionBBox) {
					// Demo: ad-hoc geography-only filter from the same bbox already
					// driving position filtering -- entity/alert type unrestricted.
					const demoScope: ScopeFilter = {
						bounds: bboxToGeoBounds(state.positionBBox),
						entity_types: null,
						alert_types: null,
					};
					if (!matchesScope(alert, demoScope)) continue;
				}
				// Demo with no positionBBox yet: unfiltered, same default as CP2's REST path.

				ws.send(envelope);
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
		const state: ConnectionState = {
			role: payload.role,
			userId: payload.user_id,
			positionBBox: null,
			operatorScope: null,
			// Demo never performs a workspace lookup, so it's "loaded" (with
			// nothing) immediately. Operator starts unloaded -- fail-closed
			// until loadOperatorScope resolves below.
			scopeLoaded: payload.role !== 'operator',
		};
		connections.set(ws, state);

		if (payload.role === 'operator') {
			loadOperatorScope(payload.user_id)
				.then((scope) => {
					const current = connections.get(ws);
					if (!current) return; // connection already closed before the lookup finished
					current.operatorScope = scope;
					current.scopeLoaded = true;
				})
				.catch((err) => {
					console.error(
						JSON.stringify({
							level: 'error',
							msg: 'failed to load operator workspace scope',
							user_id: payload.user_id,
							err: String(err),
						}),
					);
					// scopeLoaded stays false -- connection remains fail-closed for
					// alerts for its whole lifetime; a reconnect retries the load.
				});
		}

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
				total: connections.size,
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
				const current = connections.get(ws);
				if (current) current.positionBBox = { minLat, minLon, maxLat, maxLon };
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
			connections.delete(ws);
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
					total: connections.size,
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
