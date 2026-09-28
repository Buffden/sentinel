// Dependency health for GET /healthz: each dependency checked on every
// request under a time bound. The concrete checks live in index.ts.
//
// The bound matters more than the check. ioredis queues commands while it is
// disconnected, and a Postgres or Neo4j connection attempt can stall, so an
// unbounded check would hang /healthz exactly when a dependency is down.

import { log } from './logger.js';

export const HEALTH_CHECK_TIMEOUT_MS = 1_000;

export type CheckResult =
	{ ok: true } | { ok: false; reason: 'timeout' | 'not_connected' | 'error' };
export type Check = () => Promise<void>;

export interface HealthReport {
	healthy: boolean;
	checks: Record<string, CheckResult>;
}

// Thrown by a check that can tell without a round trip that its client is not
// connected, so it reports at once instead of queuing a command.
export class NotConnectedError extends Error {
	override name = 'NotConnectedError';
}

class CheckTimeoutError extends Error {
	override name = 'CheckTimeoutError';
}

// Last result per dependency, so a change is logged once rather than on every
// request. Dependencies start as healthy: only a failure is news at startup.
const lastHealthy = new Map<string, boolean>();

// A timed-out probe is not cancelled: its PING, SELECT 1 or Neo4j request
// keeps waiting on the connection. So a probe still running from an earlier
// request is joined rather than started again, which caps outstanding probes
// at one per dependency however often /healthz is called during a hang.
const inFlight = new Map<string, Promise<void>>();

function startOrJoin(name: string, check: Check): Promise<void> {
	let probe = inFlight.get(name);
	if (!probe) {
		probe = check().finally(() => inFlight.delete(name));
		inFlight.set(name, probe);
	}
	return probe;
}

async function runCheck(name: string, check: Check, timeoutMs: number): Promise<CheckResult> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const timeout = new Promise<never>((_, reject) => {
		timer = setTimeout(
			() => reject(new CheckTimeoutError(`${name} check exceeded ${timeoutMs} ms`)),
			timeoutMs,
		);
	});
	let result: CheckResult;
	let failure: unknown;
	try {
		await Promise.race([startOrJoin(name, check), timeout]);
		result = { ok: true };
	} catch (err) {
		failure = err;
		result = {
			ok: false,
			reason:
				err instanceof CheckTimeoutError
					? 'timeout'
					: err instanceof NotConnectedError
						? 'not_connected'
						: 'error',
		};
	} finally {
		clearTimeout(timer);
	}

	if ((lastHealthy.get(name) ?? true) !== result.ok) {
		if (result.ok) log('info', 'dependency healthy again', { dependency: name });
		else
			log('warn', 'dependency unhealthy', {
				dependency: name,
				reason: result.reason,
				err: failure,
			});
	}
	lastHealthy.set(name, result.ok);
	return result;
}

export async function checkHealth(
	checks: Record<string, Check>,
	timeoutMs: number = HEALTH_CHECK_TIMEOUT_MS,
): Promise<HealthReport> {
	const names = Object.keys(checks);
	const results = await Promise.all(names.map((name) => runCheck(name, checks[name]!, timeoutMs)));
	return {
		healthy: results.every((r) => r.ok),
		checks: Object.fromEntries(names.map((name, i) => [name, results[i]!])),
	};
}
