// Integration tests for SIGNAL_LOSS episode idempotency in evaluator.ts.
// Run against a REAL Redis and Kafka broker (docker-compose), not mocks: the
// guarantee under test is "one alert per continuous silence, however many
// scan ticks occur during it," which depends on real Redis EXISTS/HSET
// ordering and a real message actually landing on the `alerts` topic — a
// mocked producer would only prove the mock, not that the alert was sent.
//
// Requires: `make up` and the `alerts` topic provisioned (infra/kafka/topics.sh),
// or the CI service containers + topic-create step.
//
// WARNING — do not run this suite against a dev stack with the `api` service's
// alert-sink running. These tests publish real messages to the real `alerts`
// topic on purpose (see above); the api's Kafka consumer has no way to tell a
// test alert from a real one and will idempotently persist it to the real
// TimescaleDB `alerts` table. If that happens, the rows show up as unreadable
// `test-evaluator-<uuid>` entries in the dashboard alert panel — clean up with
// `DELETE FROM alerts WHERE entity_id LIKE 'test-%'`. Stop `api` (or point
// this suite at infra the api isn't consuming from) before running it.
import { randomUUID } from 'node:crypto';
import { Kafka } from 'kafkajs';
import {
	afterAll,
	afterEach,
	beforeAll,
	describe,
	expect,
	it,
	vi,
	type MockInstance,
} from 'vitest';
import { config } from './config.js';
import {
	handleProximityCandidate,
	producer,
	redis,
	runScan,
	startCandidateConsumerSession,
} from './evaluator.js';
import type { ProximityCandidateMessage, SignalLossScanOptions } from './evaluator.js';
import {
	CandidateDecisionConflictError,
	CompositeFinalizeInvariantError,
	claimCompositeEpisode,
	readCandidateDecision,
	writeCandidateDecisionIfAbsent,
} from './composite.js';
import type { CandidateDecision } from './composite.js';

interface AlertMessage {
	alert_id: string;
	entity_id: string;
	entity_type: string;
	alert_type: string;
	priority: string;
	status: string;
	detected_at_ms: number;
	payload: Record<string, unknown>;
}

const receivedAlerts: AlertMessage[] = [];

// Polls the in-memory buffer fed by the test consumer below for an alert
// matching entityId. Production and consumption are two independent async
// pipelines here, so a short poll (not an immediate assertion) is required.
async function waitForAlert(
	entityId: string,
	timeoutMs = 8_000,
): Promise<AlertMessage | undefined> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		const found = receivedAlerts.find((a) => a.entity_id === entityId);
		if (found) return found;
		await new Promise((r) => setTimeout(r, 100));
	}
	return undefined;
}

async function assertNoAlert(entityId: string, waitMs = 1_500): Promise<void> {
	await new Promise((r) => setTimeout(r, waitMs));
	expect(receivedAlerts.some((a) => a.entity_id === entityId)).toBe(false);
}

function seedLiveEntity(
	entityId: string,
	fields: Partial<{
		last_seen_ms: number;
		on_ground: boolean;
		entity_type: string;
		provider: string;
	}>,
): Promise<number> {
	const hashFields: string[] = [
		'entity_type',
		fields.entity_type ?? 'aircraft',
		'provider',
		fields.provider ?? 'adsbfi',
	];
	if (fields.last_seen_ms !== undefined) {
		hashFields.push('last_seen_ms', String(fields.last_seen_ms));
	}
	if (fields.on_ground !== undefined) {
		hashFields.push('on_ground', String(fields.on_ground));
	}
	return redis.hset(`entity:live:${entityId}`, ...hashFields);
}

// Each signal-loss test gets its own timeline keys and scans only its own
// entities. The shared dev Redis holds real live entities and the real
// {live-provider} timeline; neither may be read, gated or alerted here.
interface ScanScope {
	// Entity ids must start with this so the scan pattern finds them.
	prefix: string;
	options: SignalLossScanOptions;
}

function newScanScope(): ScanScope {
	const prefix = `test-evaluator-${randomUUID()}`;
	return {
		prefix,
		options: {
			authorityKey: `{${prefix}}:authority`,
			coverageKey: `{${prefix}}:coverage`,
			entityKeyPattern: `entity:live:${prefix}*`,
		},
	};
}

// An initialized authority record in the shape the coordinator writes. With
// openSinceMs, coverage is open from then until lastActiveMs; without it,
// coverage is closed and only the closed members count.
async function seedTimeline(
	scope: ScanScope,
	timeline: { lastActiveMs: number; openSinceMs?: number; closed?: string[] },
): Promise<void> {
	await redis.hset(
		scope.options.authorityKey,
		'provider',
		'adsbfi',
		'epoch',
		'1',
		'authority_since_ms',
		String(timeline.openSinceMs ?? timeline.lastActiveMs),
		'coverage_open_since_ms',
		timeline.openSinceMs === undefined ? '' : String(timeline.openSinceMs),
		'last_active_success_ms',
		String(timeline.lastActiveMs),
		'timeline_version',
		'1',
	);
	for (const member of timeline.closed ?? []) {
		const end = Number(member.split('|')[2]);
		await redis.zadd(scope.options.coverageKey, end, member);
	}
}

// adsb.fi coverage open from sinceMs up to now: enough for any entity last
// seen after sinceMs to observe its full wall-clock silence.
function seedOpenCoverageSince(scope: ScanScope, sinceMs: number): Promise<void> {
	return seedTimeline(scope, { openSinceMs: sinceMs, lastActiveMs: Date.now() });
}

async function cleanupScope(scope: ScanScope, entityIds: string[]): Promise<void> {
	await redis.del(
		scope.options.authorityKey,
		scope.options.coverageKey,
		...entityIds.flatMap((id) => [`entity:live:${id}`, `alert-state:${id}`]),
	);
}

function warnings(spy: MockInstance, message: string): unknown[][] {
	return spy.mock.calls.filter((args) => args[1] === message);
}

function buildCandidate(
	overrides: Partial<ProximityCandidateMessage> = {},
): ProximityCandidateMessage {
	const a = `test-evaluator-${randomUUID()}`;
	const b = `test-evaluator-${randomUUID()}`;
	const [entity_a_id, entity_b_id] = a <= b ? [a, b] : [b, a];
	return {
		pair_key: `${entity_a_id}:${entity_b_id}`,
		entity_a_id,
		entity_b_id,
		episode_start_ms: Date.now(),
		lat: 37.0,
		lon: -121.0,
		distance_at_detection: 33.4,
		...overrides,
	};
}

describe('evaluator.ts SIGNAL_LOSS episode idempotency (integration)', () => {
	const testKafka = new Kafka({
		clientId: 'evaluator-test',
		brokers: config.KAFKA_BROKERS,
		logLevel: 0,
	});
	const testConsumer = testKafka.consumer({ groupId: `test-alert-evaluator-${randomUUID()}` });

	beforeAll(async () => {
		await redis.ping();
		await producer.connect();

		await testConsumer.connect();
		await testConsumer.subscribe({ topic: config.ALERTS_TOPIC, fromBeginning: false });

		const joined = new Promise<void>((resolve) => {
			testConsumer.on(testConsumer.events.GROUP_JOIN, () => resolve());
		});

		void testConsumer.run({
			eachMessage: async ({ message }) => {
				if (!message.value) return;
				receivedAlerts.push(JSON.parse(message.value.toString()) as AlertMessage);
			},
		});

		await joined;
	}, 30_000);

	afterAll(async () => {
		await testConsumer.disconnect();
		await producer.disconnect();
		await redis.quit();
	});

	afterEach(() => {
		receivedAlerts.length = 0;
	});

	it('detects a dark entity and publishes a SIGNAL_LOSS alert with a deterministic id', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 10_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs });
		await seedOpenCoverageSince(scope, darkSinceMs - 1_000);
		const info = vi.spyOn(console, 'info');

		try {
			await runScan(scope.options);

			const alert = await waitForAlert(entityId);
			expect(alert).toBeDefined();
			expect(alert?.alert_id).toBe(`${entityId}:SIGNAL_LOSS:${darkSinceMs}`);
			expect(alert?.alert_type).toBe('SIGNAL_LOSS');
			expect(alert?.payload['dark_since_ms']).toBe(darkSinceMs);
			// The payload shape is unchanged: observed silence is log-only.
			expect(Object.keys(alert?.payload ?? {}).sort()).toEqual([
				'callsign',
				'dark_since_ms',
				'last_known_altitude_m',
				'last_known_course_deg',
				'last_known_lat',
				'last_known_lon',
				'last_known_speed_mps',
			]);

			const gate = await redis.hgetall(`alert-state:${entityId}`);
			expect(gate['dark_since_ms']).toBe(String(darkSinceMs));
			expect(gate['signal_loss_alert_id']).toBe(alert?.alert_id);

			const [detected] = warnings(info, 'signal loss detected');
			const fields = detected?.[0] as Record<string, unknown>;
			expect(fields['provider']).toBe('adsbfi');
			expect(fields['observed_silence_ms']).toBeGreaterThanOrEqual(config.SIGNAL_LOSS_THRESHOLD_MS);
			expect(fields['wall_silence_ms']).toBeGreaterThanOrEqual(
				fields['observed_silence_ms'] as number,
			);
			const [summary] = warnings(info, 'scan complete');
			expect((summary?.[0] as Record<string, unknown>)['timeline_version']).toBe('1');
		} finally {
			info.mockRestore();
			await cleanupScope(scope, [entityId]);
		}
	});

	it('does not re-alert on a second scan tick for the same episode', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 10_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs });
		await seedOpenCoverageSince(scope, darkSinceMs - 1_000);

		try {
			await runScan(scope.options);
			const first = await waitForAlert(entityId);
			expect(first).toBeDefined();

			// Same episode, second tick: the entity is still dark and the gate
			// from the first tick is still set.
			receivedAlerts.length = 0;
			await runScan(scope.options);
			await assertNoAlert(entityId);
		} finally {
			await cleanupScope(scope, [entityId]);
		}
	});

	it('computes a distinct alert_id for a new episode after the gate is cleared', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const firstDarkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 20_000;
		await seedLiveEntity(entityId, { last_seen_ms: firstDarkSinceMs });
		await seedOpenCoverageSince(scope, firstDarkSinceMs - 1_000);

		try {
			await runScan(scope.options);
			const first = await waitForAlert(entityId);
			expect(first).toBeDefined();

			// Simulate what the Position Consumer does when the entity resumes
			// transmitting: clear the episode gate. Then the entity goes dark
			// again at a later source timestamp — a genuinely new episode.
			await redis.del(`alert-state:${entityId}`);
			receivedAlerts.length = 0;
			const secondDarkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 5_000;
			await seedLiveEntity(entityId, { last_seen_ms: secondDarkSinceMs });

			await runScan(scope.options);
			const second = await waitForAlert(entityId);
			expect(second).toBeDefined();
			expect(second?.alert_id).not.toBe(first?.alert_id);
			expect(second?.alert_id).toBe(`${entityId}:SIGNAL_LOSS:${secondDarkSinceMs}`);
		} finally {
			await cleanupScope(scope, [entityId]);
		}
	});

	it('does not alert an entity that is on the ground', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 10_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs, on_ground: true });
		await seedOpenCoverageSince(scope, darkSinceMs - 1_000);

		try {
			await runScan(scope.options);
			await assertNoAlert(entityId);
			expect(await redis.exists(`alert-state:${entityId}`)).toBe(0);
		} finally {
			await cleanupScope(scope, [entityId]);
		}
	});

	it('does not alert an entity with no accepted position (missing last_seen_ms)', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		await seedLiveEntity(entityId, {});
		await seedOpenCoverageSince(scope, Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS * 2);

		try {
			await runScan(scope.options);
			await assertNoAlert(entityId);
		} finally {
			await cleanupScope(scope, [entityId]);
		}
	});

	it('does not alert an entity that is still within the silence threshold', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const recentMs = Date.now() - Math.floor(config.SIGNAL_LOSS_THRESHOLD_MS / 2);
		await seedLiveEntity(entityId, { last_seen_ms: recentMs });
		await seedOpenCoverageSince(scope, recentMs - 1_000);

		try {
			await runScan(scope.options);
			await assertNoAlert(entityId);
			expect(await redis.exists(`alert-state:${entityId}`)).toBe(0);
		} finally {
			await cleanupScope(scope, [entityId]);
		}
	});

	it('does not alert when wall silence exceeds the threshold but observed silence does not', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const now = Date.now();
		const darkSinceMs = now - config.SIGNAL_LOSS_THRESHOLD_MS - 60_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs });
		// The provider covered only the most recent half-threshold of the silence;
		// before that it was down, so that time proves nothing about the aircraft.
		await seedTimeline(scope, {
			openSinceMs: now - Math.floor(config.SIGNAL_LOSS_THRESHOLD_MS / 2),
			lastActiveMs: now,
		});

		try {
			await runScan(scope.options);
			await assertNoAlert(entityId);
			expect(await redis.exists(`alert-state:${entityId}`)).toBe(0);
		} finally {
			await cleanupScope(scope, [entityId]);
		}
	});

	it('alerts on covered silence split across a closed segment and the open span', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const now = Date.now();
		const threshold = config.SIGNAL_LOSS_THRESHOLD_MS;
		const darkSinceMs = now - threshold - 120_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs });
		// Closed: darkSince .. darkSince + threshold/2. Then a 60 s outage.
		// Open: the remaining threshold/2 + 1 s. Together, just over threshold.
		const closedEnd = darkSinceMs + threshold / 2;
		await seedTimeline(scope, {
			closed: [`adsbfi|${darkSinceMs}|${closedEnd}|failure`],
			openSinceMs: closedEnd + 60_000,
			lastActiveMs: closedEnd + 60_000 + threshold / 2 + 1_000,
		});

		try {
			await runScan(scope.options);
			const alert = await waitForAlert(entityId);
			expect(alert?.alert_id).toBe(`${entityId}:SIGNAL_LOSS:${darkSinceMs}`);
		} finally {
			await cleanupScope(scope, [entityId]);
		}
	});

	it('does not alert when the timeline is uninitialized, and warns once per scan', async () => {
		const scope = newScanScope();
		const ids = [`${scope.prefix}-a`, `${scope.prefix}-b`];
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 60_000;
		for (const id of ids) await seedLiveEntity(id, { last_seen_ms: darkSinceMs });
		// A closed segment with no authority record is not trusted either.
		await redis.zadd(
			scope.options.coverageKey,
			Date.now(),
			`adsbfi|${darkSinceMs - 1_000}|${Date.now()}|failure`,
		);
		const warn = vi.spyOn(console, 'warn');

		try {
			await runScan(scope.options);
			for (const id of ids) await assertNoAlert(id, 500);
			const suppressed = warnings(
				warn,
				'signal loss suppressed: provider timeline not initialized',
			);
			expect(suppressed).toHaveLength(1);
			expect(suppressed[0]?.[0]).toMatchObject({
				scanned: 2,
				eligible: 2,
				wall_silent_over_threshold: 2,
			});
		} finally {
			warn.mockRestore();
			await cleanupScope(scope, ids);
		}
	});

	it('treats a heartbeat-only authority record as uninitialized', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 60_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs });
		// What a coordinator leaves when it held the lease but never credited a cycle.
		await redis.hset(scope.options.authorityKey, 'heartbeat_ms', String(Date.now()));
		const warn = vi.spyOn(console, 'warn');

		try {
			await runScan(scope.options);
			await assertNoAlert(entityId);
			expect(
				warnings(warn, 'signal loss suppressed: provider timeline not initialized'),
			).toHaveLength(1);
		} finally {
			warn.mockRestore();
			await cleanupScope(scope, [entityId]);
		}
	});

	it('does not alert an OpenSky-owned aircraft on adsb.fi-only coverage', async () => {
		const scope = newScanScope();
		const openskyId = `${scope.prefix}-opensky`;
		const adsbfiId = `${scope.prefix}-adsbfi`;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 10_000;
		await seedLiveEntity(openskyId, { last_seen_ms: darkSinceMs, provider: 'opensky' });
		// Control: same silence, same scan, owned by the covering provider.
		await seedLiveEntity(adsbfiId, { last_seen_ms: darkSinceMs, provider: 'adsbfi' });
		await seedOpenCoverageSince(scope, darkSinceMs - 1_000);

		try {
			await runScan(scope.options);
			expect(await waitForAlert(adsbfiId)).toBeDefined();
			await assertNoAlert(openskyId, 500);
			expect(await redis.exists(`alert-state:${openskyId}`)).toBe(0);
		} finally {
			await cleanupScope(scope, [openskyId, adsbfiId]);
		}
	});

	it('leaves an existing episode gate untouched and does not re-alert', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 10_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs });
		await seedOpenCoverageSince(scope, darkSinceMs - 1_000);
		const gate = {
			dark_since_ms: String(darkSinceMs),
			signal_loss_alert_id: `${entityId}:SIGNAL_LOSS:${darkSinceMs}`,
			composite_issued: '1',
		};
		await redis.hset(`alert-state:${entityId}`, gate);

		try {
			await runScan(scope.options);
			await assertNoAlert(entityId);
			expect(await redis.hgetall(`alert-state:${entityId}`)).toEqual(gate);
		} finally {
			await cleanupScope(scope, [entityId]);
		}
	});

	it('skips signal loss without a wall-clock fallback when the timeline cannot be read', async () => {
		const scope = newScanScope();
		const entityId = scope.prefix;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 60_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs });
		const error = vi.spyOn(console, 'error');

		try {
			// Wrong key type: HGETALL fails inside EXEC with WRONGTYPE.
			await redis.set(scope.options.authorityKey, 'not-a-hash');
			await runScan(scope.options);
			await assertNoAlert(entityId);

			// Valid authority, but ZRANGE fails the same way.
			await redis.del(scope.options.authorityKey);
			await seedOpenCoverageSince(scope, darkSinceMs - 1_000);
			await redis.set(scope.options.coverageKey, 'not-a-zset');
			await runScan(scope.options);
			await assertNoAlert(entityId);

			expect(await redis.exists(`alert-state:${entityId}`)).toBe(0);
			const failures = warnings(error, 'signal loss skipped: provider timeline snapshot failed');
			expect(failures.map((c) => (c[0] as Record<string, unknown>)['detail'])).toEqual([
				'authority read failed',
				'coverage read failed',
			]);
		} finally {
			error.mockRestore();
			await cleanupScope(scope, [entityId]);
		}
	});

	// ---- handleProximityCandidate ------------------------------------------
	// Same real Redis + real `alerts` topic setup as the SIGNAL_LOSS suite
	// above -- the guarantee under test is that a real proximity.candidates
	// payload becomes a real, deterministic UNSCHEDULED_PROXIMITY alert.

	it('emits a deterministic UNSCHEDULED_PROXIMITY alert from a proximity candidate', async () => {
		const candidate = buildCandidate();
		await seedLiveEntity(candidate.entity_a_id, { entity_type: 'aircraft' });

		try {
			await handleProximityCandidate(candidate);

			const alert = await waitForAlert(candidate.entity_a_id);
			expect(alert).toBeDefined();
			expect(alert?.alert_id).toBe(
				`${candidate.pair_key}:UNSCHEDULED_PROXIMITY:${candidate.episode_start_ms}`,
			);
			expect(alert?.alert_type).toBe('UNSCHEDULED_PROXIMITY');
			expect(alert?.entity_type).toBe('aircraft');
			expect(alert?.payload['pair_key']).toBe(candidate.pair_key);
			expect(alert?.payload['counterparty_entity_id']).toBe(candidate.entity_b_id);
			expect(alert?.payload['distance_metres']).toBe(candidate.distance_at_detection);
		} finally {
			await redis.del(`entity:live:${candidate.entity_a_id}`);
		}
	});

	it('defaults entity_type to an empty string when the entity has no live state', async () => {
		const candidate = buildCandidate(); // entity_a_id deliberately never seeded

		const alert = (await (async () => {
			await handleProximityCandidate(candidate);
			return waitForAlert(candidate.entity_a_id);
		})())!;

		expect(alert.entity_type).toBe('');
	});

	it('computes the same alert_id regardless of which run produced the candidate', async () => {
		// The Correlation Worker already canonicalizes entity_a_id/entity_b_id,
		// so a redelivered candidate for the same episode has identical fields
		// -- this only confirms handleProximityCandidate does not add its own
		// nondeterminism on top (e.g. from Date.now() leaking into alert_id).
		const candidate = buildCandidate();
		await seedLiveEntity(candidate.entity_a_id, { entity_type: 'vessel' });

		try {
			await handleProximityCandidate(candidate);
			const first = await waitForAlert(candidate.entity_a_id);

			receivedAlerts.length = 0;
			await handleProximityCandidate(candidate);
			const second = await waitForAlert(candidate.entity_a_id);

			expect(first?.alert_id).toBe(second?.alert_id);
		} finally {
			await redis.del(`entity:live:${candidate.entity_a_id}`);
		}
	});

	// ---- handleProximityCandidate -- composite correlation (Pre-CP5A) -----
	// Nested inside this describe (not a sibling) so it shares the outer
	// beforeAll/afterAll: the real producer connection and the real `alerts`
	// topic consumer feeding receivedAlerts are only alive for this
	// describe's lifetime, and a sibling describe run after this one closes
	// would find both already torn down.
	describe('handleProximityCandidate composite correlation (Pre-CP5A) (integration)', () => {
		function seedActiveLoss(entityId: string, darkSinceMs: number): Promise<unknown> {
			return redis.hset(`alert-state:${entityId}`, {
				dark_since_ms: String(darkSinceMs),
				signal_loss_alert_id: `${entityId}:SIGNAL_LOSS:${darkSinceMs}`,
				composite_issued: '0',
			});
		}

		function seedRecentLoss(
			entityId: string,
			darkSinceMs: number,
			resumedAtMs: number,
		): Promise<unknown> {
			return redis.hset(`recent-loss:${entityId}`, {
				dark_since_ms: String(darkSinceMs),
				resumed_at_ms: String(resumedAtMs),
				signal_loss_alert_id: `${entityId}:SIGNAL_LOSS:${darkSinceMs}`,
				composite_issued: '0',
			});
		}

		afterEach(() => {
			receivedAlerts.length = 0;
		});

		it('a fresh candidate with a qualifying RECENT loss on entity_b becomes COMPOSITE, entity_type sourced from the selected entity', async () => {
			const candidate = buildCandidate();
			const candidateId = `${candidate.pair_key}:${candidate.episode_start_ms}`;
			const darkSinceMs = candidate.episode_start_ms - 8_000;
			const resumedAtMs = candidate.episode_start_ms - 3_000;
			await seedRecentLoss(candidate.entity_b_id, darkSinceMs, resumedAtMs);
			// entity_a deliberately seeded with a DIFFERENT entity_type, so a
			// passing test proves entity_type came from entity_b (the selected
			// entity), not from the UNSCHEDULED_PROXIMITY convention of always
			// reading entity_a.
			await seedLiveEntity(candidate.entity_a_id, { entity_type: 'vessel' });
			await seedLiveEntity(candidate.entity_b_id, { entity_type: 'aircraft' });

			try {
				await handleProximityCandidate(candidate);

				const alert = await waitForAlert(candidate.entity_b_id);
				expect(alert).toBeDefined();
				expect(alert?.alert_type).toBe('COMPOSITE');
				expect(alert?.alert_id).toBe(`${candidate.pair_key}:COMPOSITE:${darkSinceMs}`);
				expect(alert?.priority).toBe('ELEVATED');
				expect(alert?.entity_type).toBe('aircraft');
				expect(alert?.payload['supersedes_alert_ids']).toEqual([
					`${candidate.entity_b_id}:SIGNAL_LOSS:${darkSinceMs}`,
				]);
				const signalLoss = alert?.payload['signal_loss'] as Record<string, unknown>;
				expect(signalLoss['loss_source']).toBe('RECENT');
				expect(signalLoss['resumed_at_ms']).toBe(resumedAtMs);

				const state = await redis.hgetall(`recent-loss:${candidate.entity_b_id}`);
				expect(state['composite_issued']).toBe('1');
				expect(state['composite_claim_candidate_id']).toBe(candidateId);
			} finally {
				await redis.del(
					`entity:live:${candidate.entity_a_id}`,
					`entity:live:${candidate.entity_b_id}`,
					`recent-loss:${candidate.entity_b_id}`,
					`alert-decision:${candidateId}`,
				);
			}
		});

		// Signal loss runs in runScan; proximity and composite run in the
		// candidate consumer. A timeline read failure must disable only the
		// first, and must not throw out of the leader loop, which would stop
		// the second.
		it.each(['authority', 'coverage'] as const)(
			'a %s read failure suppresses only signal loss; proximity and composite still run',
			async (broken) => {
				const scope = newScanScope();
				const darkId = scope.prefix;
				const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 60_000;
				await seedLiveEntity(darkId, { last_seen_ms: darkSinceMs });
				await seedOpenCoverageSince(scope, darkSinceMs - 1_000);
				const brokenKey =
					broken === 'authority' ? scope.options.authorityKey : scope.options.coverageKey;
				await redis.del(brokenKey);
				await redis.set(brokenKey, 'wrong-type');

				const unscheduled = buildCandidate();
				const composite = buildCandidate();
				const compositeDarkSinceMs = composite.episode_start_ms - 8_000;
				// Active loss raised by an earlier, healthy scan.
				await seedActiveLoss(composite.entity_a_id, compositeDarkSinceMs);
				await seedLiveEntity(composite.entity_a_id, { entity_type: 'aircraft' });
				const decisionKeys = [unscheduled, composite].map(
					(c) => `alert-decision:${c.pair_key}:${c.episode_start_ms}`,
				);

				try {
					// One leader tick: the scan, then candidates from the consumer.
					await expect(runScan(scope.options)).resolves.toBeUndefined();
					await handleProximityCandidate(unscheduled);
					await handleProximityCandidate(composite);

					const proximityAlert = await waitForAlert(unscheduled.entity_a_id);
					expect(proximityAlert?.alert_type).toBe('UNSCHEDULED_PROXIMITY');

					const compositeAlert = await waitForAlert(composite.entity_a_id);
					expect(compositeAlert?.alert_type).toBe('COMPOSITE');
					expect(compositeAlert?.alert_id).toBe(
						`${composite.pair_key}:COMPOSITE:${compositeDarkSinceMs}`,
					);
					const state = await redis.hgetall(`alert-state:${composite.entity_a_id}`);
					expect(state['composite_issued']).toBe('1');

					await assertNoAlert(darkId, 500);
					expect(await redis.exists(`alert-state:${darkId}`)).toBe(0);
				} finally {
					await cleanupScope(scope, [darkId]);
					await redis.del(
						`entity:live:${unscheduled.entity_a_id}`,
						`entity:live:${composite.entity_a_id}`,
						`alert-state:${composite.entity_a_id}`,
						...decisionKeys,
					);
				}
			},
		);

		it('replaying an existing COMPOSITE decision republishes and finalizes again (idempotent)', async () => {
			const candidate = buildCandidate();
			const candidateId = `${candidate.pair_key}:${candidate.episode_start_ms}`;
			const darkSinceMs = candidate.episode_start_ms - 8_000;
			await seedActiveLoss(candidate.entity_a_id, darkSinceMs);
			await seedLiveEntity(candidate.entity_a_id, { entity_type: 'aircraft' });

			try {
				await handleProximityCandidate(candidate);
				const first = await waitForAlert(candidate.entity_a_id);
				expect(first?.alert_type).toBe('COMPOSITE');

				receivedAlerts.length = 0;
				await handleProximityCandidate(candidate); // redelivery: decision record already exists

				const second = await waitForAlert(candidate.entity_a_id);
				expect(second?.alert_id).toBe(first?.alert_id);

				// Still finalized, not corrupted or double-mutated by the replay.
				const state = await redis.hgetall(`alert-state:${candidate.entity_a_id}`);
				expect(state['composite_issued']).toBe('1');
			} finally {
				await redis.del(
					`entity:live:${candidate.entity_a_id}`,
					`alert-state:${candidate.entity_a_id}`,
					`alert-decision:${candidateId}`,
				);
			}
		});

		it('CLAIM failure decides UNSCHEDULED_PROXIMITY with no fallback to the other pair member, even when it independently qualifies', async () => {
			const candidate = buildCandidate();
			const candidateId = `${candidate.pair_key}:${candidate.episode_start_ms}`;
			// entity_a has the smaller gap_ms so CP2's own tie-break picks it
			// as the winner -- the pre-claim below must land on the entity CP2
			// will actually select, not merely on entity_a by coincidence.
			const darkSinceMsA = candidate.episode_start_ms - 6_000;
			const darkSinceMsB = candidate.episode_start_ms - 8_000;
			await seedActiveLoss(candidate.entity_a_id, darkSinceMsA);
			await seedActiveLoss(candidate.entity_b_id, darkSinceMsB); // also qualifies
			await seedLiveEntity(candidate.entity_a_id, { entity_type: 'aircraft' });
			// A different candidate already holds entity_a's claim (the
			// tie-break winner, smaller gap_ms) -- CLAIM must fail here.
			await claimCompositeEpisode(redis, candidate.entity_a_id, darkSinceMsA, 'someone-else:1');

			try {
				await handleProximityCandidate(candidate);

				const alert = await waitForAlert(candidate.entity_a_id);
				expect(alert?.alert_type).toBe('UNSCHEDULED_PROXIMITY');

				// entity_a's claim is untouched, still owned by the other candidate.
				const stateA = await redis.hgetall(`alert-state:${candidate.entity_a_id}`);
				expect(stateA['composite_claim_candidate_id']).toBe('someone-else:1');
				expect(stateA['composite_issued']).toBe('0');

				// No fallback attempt against entity_b: its episode is untouched,
				// never claimed by this candidate_id.
				const stateB = await redis.hgetall(`alert-state:${candidate.entity_b_id}`);
				expect(stateB['composite_claim_candidate_id'] ?? '').toBe('');
				expect(stateB['composite_issued']).toBe('0');
			} finally {
				await redis.del(
					`entity:live:${candidate.entity_a_id}`,
					`alert-state:${candidate.entity_a_id}`,
					`alert-state:${candidate.entity_b_id}`,
					`alert-decision:${candidateId}`,
				);
			}
		});

		// Pre-CP5A(b): raced via real Promise.all against a directly-written
		// competing decision for the SAME candidate_id, not sequenced or mocked
		// -- structured to be correct regardless of which side actually wins
		// the real race, since which one wins is a genuine timing outcome, not
		// something this test controls.
		it('a decision-write conflict after a successful CLAIM releases the stray claim and converges on one decision', async () => {
			const candidate = buildCandidate();
			const candidateId = `${candidate.pair_key}:${candidate.episode_start_ms}`;
			const darkSinceMs = candidate.episode_start_ms - 8_000;
			await seedActiveLoss(candidate.entity_a_id, darkSinceMs);
			await seedLiveEntity(candidate.entity_a_id, { entity_type: 'aircraft' });

			const competingDecision: CandidateDecision = {
				decision: 'UNSCHEDULED_PROXIMITY',
				candidate_id: candidateId,
			};

			try {
				const [, otherOutcome] = await Promise.all([
					handleProximityCandidate(candidate),
					writeCandidateDecisionIfAbsent(redis, competingDecision).catch((err: unknown) => err),
				]);

				const stored = await readCandidateDecision(redis, candidateId);
				expect(stored).not.toBeNull();

				if (stored?.decision === 'UNSCHEDULED_PROXIMITY') {
					// The direct write won: handleProximityCandidate's own CLAIM on
					// entity_a must have been released after losing the conflict,
					// not left dangling under a candidate_id that will never
					// finalize it.
					const state = await redis.hgetall(`alert-state:${candidate.entity_a_id}`);
					expect(state['composite_claim_candidate_id'] ?? '').toBe('');
					expect(state['composite_issued']).toBe('0');

					const alert = await waitForAlert(candidate.entity_a_id);
					expect(alert?.alert_type).toBe('UNSCHEDULED_PROXIMITY');
				} else {
					// handleProximityCandidate's own COMPOSITE decision won instead:
					// the direct competing write must have hit the conflict.
					expect(otherOutcome).toBeInstanceOf(CandidateDecisionConflictError);

					const alert = await waitForAlert(candidate.entity_a_id);
					expect(alert?.alert_type).toBe('COMPOSITE');

					const state = await redis.hgetall(`alert-state:${candidate.entity_a_id}`);
					expect(state['composite_issued']).toBe('1');
				}
			} finally {
				await redis.del(
					`entity:live:${candidate.entity_a_id}`,
					`alert-state:${candidate.entity_a_id}`,
					`alert-decision:${candidateId}`,
				);
			}
		});

		it('replaying a COMPOSITE decision whose claim was never actually held throws and never marks the episode issued', async () => {
			const candidate = buildCandidate();
			const candidateId = `${candidate.pair_key}:${candidate.episode_start_ms}`;
			const darkSinceMs = candidate.episode_start_ms - 8_000;
			await seedActiveLoss(candidate.entity_a_id, darkSinceMs);
			await seedLiveEntity(candidate.entity_a_id, { entity_type: 'aircraft' });

			// A decision record exists -- CP3C guarantees replay uses it -- but no
			// CLAIM was ever actually placed under this candidate_id. An
			// invariant violation FINALIZE must catch, not silently accept.
			await writeCandidateDecisionIfAbsent(redis, {
				decision: 'COMPOSITE',
				candidate_id: candidateId,
				selected_entity_id: candidate.entity_a_id,
				loss_source: 'ACTIVE',
				dark_since_ms: darkSinceMs,
				signal_loss_alert_id: `${candidate.entity_a_id}:SIGNAL_LOSS:${darkSinceMs}`,
				resumed_at_ms: null,
			});

			try {
				await expect(handleProximityCandidate(candidate)).rejects.toBeInstanceOf(
					CompositeFinalizeInvariantError,
				);

				// The alert was still published before FINALIZE ran (Pre-CP5A(c):
				// NOT_CLAIMED is caught after publish, not before it) -- but the
				// episode itself was never marked issued, since FINALIZE never
				// succeeded, and the invariant error is what must stop the caller
				// from proceeding to commit.
				const alert = await waitForAlert(candidate.entity_a_id);
				expect(alert?.alert_type).toBe('COMPOSITE');

				const state = await redis.hgetall(`alert-state:${candidate.entity_a_id}`);
				expect(state['composite_issued']).toBe('0');
			} finally {
				await redis.del(
					`entity:live:${candidate.entity_a_id}`,
					`alert-state:${candidate.entity_a_id}`,
					`alert-decision:${candidateId}`,
				);
			}
		});
	});
});

// ADR-005: only the current lease holder joins/polls the Alert Evaluator's
// candidate consumer group. This tests the real Kafka group protocol (real
// Redpanda, real JoinGroup/LeaveGroup with the broker's group coordinator) --
// a mocked consumer cannot prove real membership changes.
//
// Every test here uses its own disposable groupId, generated per test, NEVER
// config.GROUP_ID: joining the real alert-evaluator group here would trigger
// a real rebalance against any dev evaluator instance that happens to be
// running against this same broker.
describe('startCandidateConsumerSession — ADR-005 candidate consumer group lifecycle (integration)', () => {
	const testKafka = new Kafka({
		clientId: 'evaluator-session-test',
		brokers: config.KAFKA_BROKERS,
		logLevel: 0,
	});
	const admin = testKafka.admin();

	beforeAll(async () => {
		await admin.connect();
	});

	afterAll(async () => {
		await admin.disconnect();
	});

	// Kafka group join/leave and coordinator state changes are asynchronous --
	// describeGroups() called immediately after connect()/disconnect() can
	// still reflect the pre-change state. Poll with a bounded timeout instead
	// of asserting once.
	async function waitForMemberCount(
		groupId: string,
		expected: number,
		timeoutMs = 10_000,
	): Promise<number> {
		const deadline = Date.now() + timeoutMs;
		let lastCount = -1;
		while (Date.now() < deadline) {
			const { groups } = await admin.describeGroups([groupId]);
			lastCount = groups[0]?.members.length ?? 0;
			if (lastCount === expected) return lastCount;
			await new Promise((r) => setTimeout(r, 200));
		}
		return lastCount;
	}

	it('joining a session makes this instance the sole real Kafka group member', async () => {
		const groupId = `test-alert-evaluator-session-${randomUUID()}`;
		const session = await startCandidateConsumerSession(groupId);
		try {
			expect(await waitForMemberCount(groupId, 1)).toBe(1);
		} finally {
			await session.stop();
		}
	}, 20_000);

	it('stopping the session actually leaves the real Kafka group', async () => {
		const groupId = `test-alert-evaluator-session-${randomUUID()}`;
		const session = await startCandidateConsumerSession(groupId);
		expect(await waitForMemberCount(groupId, 1)).toBe(1);

		await session.stop();

		expect(await waitForMemberCount(groupId, 0)).toBe(0);
	}, 20_000);

	it('stop() is idempotent under concurrent callers', async () => {
		const groupId = `test-alert-evaluator-session-${randomUUID()}`;
		const session = await startCandidateConsumerSession(groupId);
		await waitForMemberCount(groupId, 1);

		// Mirrors the real shape: the lease-loss callback calls stop() and
		// runLeaderSession's own teardown calls it too -- both must converge
		// on one disconnect rather than racing a second one.
		await Promise.all([session.stop(), session.stop()]);

		expect(await waitForMemberCount(groupId, 0)).toBe(0);
	}, 20_000);
});
