// Runtime boundary verification for provider failover.
//
// Provider HTTP is injected so this suite spends zero OpenSky credits, while
// Redis and Kafka are real. The point is to verify the boundaries between
// health, publication, authority and coverage with the same coordinator and
// Redis scripts used by production.
import { randomUUID } from 'node:crypto';
import { Redis } from 'ioredis';
import { Kafka, Partitioners } from 'kafkajs';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type { AdsbfiFetchFailure, SplitResult } from './adsbfiPoller.js';
import { Coordinator, type CoordinatorDeps } from './coordinator.js';
import { CoordinatorLease } from './coordinatorLease.js';
import { CoverageTimeline } from './coverageTimeline.js';
import type { OpenskyFetchResult } from './poller.js';
import { toHashFields, type Provider, type ProviderHealth } from './providerHealth.js';
import { ProviderHealthStore } from './providerHealthStore.js';

const REDIS_URL = process.env['REDIS_URL'] ?? 'redis://localhost:6379';
const KAFKA_BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');

const redis = new Redis(REDIS_URL);
const kafka = new Kafka({
	clientId: 'ingestion-failover-integration',
	brokers: KAFKA_BROKERS,
	logLevel: 0,
});
const admin = kafka.admin();
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

let tag: string;
let leaseKey: string;
let authorityKey: string;
let coverageKey: string;
let healthKeys: Record<Provider, string>;
let topic: string;

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function waitFor(
	condition: () => boolean | Promise<boolean>,
	timeoutMs = 5_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!(await condition())) {
		if (Date.now() > deadline) throw new Error(`condition not met within ${timeoutMs} ms`);
		await sleep(20);
	}
}

async function topicHighWatermark(): Promise<number> {
	const offsets = await admin.fetchTopicOffsets(topic);
	return Number(offsets.find((o) => o.partition === 0)?.high ?? '0');
}

function message(provider: Provider, sequence: number) {
	return {
		key: `${provider}-${sequence}`,
		value: JSON.stringify({ provider, sequence }),
	};
}

function healthy(provider: Provider, atMs: number): ProviderHealth {
	return {
		state: 'HEALTHY',
		stateSinceMs: atMs,
		lastSuccessMs: atMs,
		lastFailureMs: null,
		consecutiveFailures: 0,
		lastError: null,
		successStreakSinceMs: null,
		pausedUntilMs: null,
		creditsRemaining: provider === 'opensky' ? 399 : null,
		lastProbeMs: provider === 'opensky' ? atMs : null,
	};
}

interface Runtime {
	coordinator: Coordinator;
	client: Redis;
	acceptedProviders: Provider[];
	getMaxPublishInFlight: () => number;
	logs: string[];
}

function runtime(
	fetchCycle: () => Promise<SplitResult | AdsbfiFetchFailure>,
	fetchOpensky: () => Promise<OpenskyFetchResult>,
	publishOverride?: CoordinatorDeps['publish'],
): Runtime {
	const client = new Redis(REDIS_URL, { commandTimeout: 2_000 });
	const acceptedProviders: Provider[] = [];
	const logs: string[] = [];
	let publishInFlight = 0;
	let maxPublishInFlight = 0;

	const brokerPublish: CoordinatorDeps['publish'] = async (messages) => {
		publishInFlight++;
		maxPublishInFlight = Math.max(maxPublishInFlight, publishInFlight);
		try {
			// Keep a publish in flight briefly so concurrent provider loops have
			// an opportunity to contend for the coordinator's publication lane.
			await sleep(10);
			const results = await producer.send({ topic, messages });
			for (const item of messages) {
				const parsed = JSON.parse(String(item.value)) as { provider: Provider };
				acceptedProviders.push(parsed.provider);
			}
			return results[0]?.baseOffset ?? 'unknown';
		} finally {
			publishInFlight--;
		}
	};

	const coordinator = new Coordinator({
		lease: new CoordinatorLease(client, 3_000, leaseKey, authorityKey),
		timeline: new CoverageTimeline(client, 100_000, leaseKey, authorityKey, coverageKey),
		health: new ProviderHealthStore(client, leaseKey, authorityKey, healthKeys),
		fetchCycle,
		fetchOpensky,
		openskyAuthenticated: true,
		healthTiming: { degradedTimeoutMs: 150, recoveryWindowMs: 150 },
		openskyCadence: {
			healthyMs: 50,
			degradedMs: 30,
			recoveringMs: 30,
			backoffBaseMs: 60,
			backoffMaxMs: 120,
		},
		publish: publishOverride ?? brokerPublish,
		log: (_level, text, extra) => logs.push(`${text} ${JSON.stringify(extra ?? {})}`),
		renewalIntervalMs: 500,
		followerRetryMs: 500,
		pollIntervalMs: 30,
		backoffBaseMs: 30,
		backoffMaxMs: 60,
		frozenFeedMs: 1_000,
		adsbfiStandbyIntervalMs: 30,
		openskyActiveIntervalMs: 30,
		selectionRetryBaseMs: 80,
		selectionRetryMaxMs: 320,
	});

	return {
		coordinator,
		client,
		acceptedProviders,
		getMaxPublishInFlight: () => maxPublishInFlight,
		logs,
	};
}

beforeAll(async () => {
	await admin.connect();
	await producer.connect();
});

beforeEach(async () => {
	tag = `{test-failover-${randomUUID()}}`;
	leaseKey = `${tag}:lease`;
	authorityKey = `${tag}:authority`;
	coverageKey = `${tag}:coverage`;
	healthKeys = {
		adsbfi: `${tag}:health:adsbfi`,
		opensky: `${tag}:health:opensky`,
	};
	topic = `adsb-raw-failover-${randomUUID()}`;
	await admin.createTopics({
		topics: [{ topic, numPartitions: 1, replicationFactor: 1 }],
		waitForLeaders: true,
	});
});

afterEach(async () => {
	await redis.del(leaseKey, authorityKey, coverageKey, healthKeys.adsbfi, healthKeys.opensky);
	await admin.deleteTopics({ topics: [topic] }).catch(() => undefined);
});

afterAll(async () => {
	await producer.disconnect();
	await admin.disconnect();
	await redis.quit();
});

describe('Coordinator failover against real Redis and Kafka', () => {
	it('moves adsb.fi -> none -> OpenSky, does not fail back while OpenSky is healthy, then recovers to adsb.fi', async () => {
		let adsbDown = false;
		let openskyDown = false;
		let adsbSequence = 0;
		let openskySequence = 0;
		let responseNowMs = 1_790_400_000_000;

		let releaseOpenSky!: () => void;
		const openSkyGate = new Promise<void>((resolve) => {
			releaseOpenSky = resolve;
		});
		let holdOpenSkyCandidate = true;

		let releaseAdsb!: () => void;
		const adsbGate = new Promise<void>((resolve) => {
			releaseAdsb = resolve;
		});
		let holdAdsbCandidate = false;

		const r = runtime(
			async () => {
				if (adsbDown) return { error: 'http_503' };
				if (holdAdsbCandidate && (await redis.hget(authorityKey, 'provider')) === 'none') {
					await adsbGate;
				}
				return {
					messages: [message('adsbfi', ++adsbSequence)],
					responseNowMs: (responseNowMs += 1_000),
					total: 1,
					skippedNonIcao: 0,
					skippedNoPosition: 0,
					skippedOutsideBox: 0,
				};
			},
			async () => {
				if (
					holdOpenSkyCandidate &&
					(await redis.hget(authorityKey, 'provider')) === 'none'
				) {
					await openSkyGate;
				}
				if (openskyDown) return { kind: 'failed', error: 'http_503' };
				return {
					kind: 'ok',
					messages: [message('opensky', ++openskySequence)],
					responseTime: Math.floor(Date.now() / 1_000),
					creditsRemaining: 399,
				};
			},
		);

		let stopped = false;
		try {
			r.coordinator.start();

			// First valid adsb.fi response publishes and COMMITs epoch 1, but
			// its seeded freshness does not CREDIT coverage.
			await waitFor(async () => (await redis.hget(authorityKey, 'provider')) === 'adsbfi');
			expect(await redis.hget(authorityKey, 'epoch')).toBe('1');
			expect(await redis.hget(authorityKey, 'coverage_open_since_ms')).toBe('');

			// The next advancing response is fresh and opens coverage separately.
			await waitFor(async () => (await redis.hget(authorityKey, 'coverage_open_since_ms')) !== '');
			expect(await redis.hget(authorityKey, 'timeline_version')).toBe('2');

			// Provider failure closes coverage, reaches UNAVAILABLE, then
			// RELINQUISH writes literal none without changing epoch.
			adsbDown = true;
			await waitFor(async () => (await redis.hget(authorityKey, 'provider')) === 'none');
			const noneAfterAdsb = await redis.hgetall(authorityKey);
			expect(noneAfterAdsb).toMatchObject({
				provider: 'none',
				epoch: '1',
				coverage_open_since_ms: '',
				timeline_version: '4',
			});

			// Release the warm OpenSky candidate: its successful Kafka publish
			// COMMITs epoch 2, then CREDIT opens coverage.
			holdOpenSkyCandidate = false;
			releaseOpenSky();
			await waitFor(async () => (await redis.hget(authorityKey, 'provider')) === 'opensky');
			await waitFor(async () => (await redis.hget(authorityKey, 'coverage_open_since_ms')) !== '');
			expect(await redis.hget(authorityKey, 'epoch')).toBe('2');
			expect(await redis.hget(authorityKey, 'timeline_version')).toBe('6');

			// adsb.fi may recover all the way to HEALTHY while OpenSky remains
			// healthy; CP3e must not proactively fail back.
			adsbDown = false;
			const adsbPublishedBeforeRecovery = r.acceptedProviders.filter((p) => p === 'adsbfi').length;
			await waitFor(async () => (await redis.hget(healthKeys.adsbfi, 'state')) === 'HEALTHY');
			await sleep(100);
			expect(await redis.hget(authorityKey, 'provider')).toBe('opensky');
			expect(r.acceptedProviders.filter((p) => p === 'adsbfi')).toHaveLength(
				adsbPublishedBeforeRecovery,
			);

			// Only after OpenSky itself becomes UNAVAILABLE may the recovered
			// adsb.fi provider take authority. Hold that candidate long enough
			// to inspect the intermediate none record.
			holdAdsbCandidate = true;
			openskyDown = true;
			await waitFor(async () => (await redis.hget(authorityKey, 'provider')) === 'none');
			const noneAfterOpenSky = await redis.hgetall(authorityKey);
			expect(noneAfterOpenSky['epoch']).toBe('2');
			expect(noneAfterOpenSky['coverage_open_since_ms']).toBe('');

			holdAdsbCandidate = false;
			releaseAdsb();
			await waitFor(async () => (await redis.hget(authorityKey, 'provider')) === 'adsbfi');
			await waitFor(async () => (await redis.hget(authorityKey, 'coverage_open_since_ms')) !== '');
			expect(await redis.hget(authorityKey, 'epoch')).toBe('3');

			// The real Kafka producer was used for every successful delivery,
			// and the coordinator never intentionally had two publishes active.
			expect(r.acceptedProviders).toContain('adsbfi');
			expect(r.acceptedProviders).toContain('opensky');
			expect(r.getMaxPublishInFlight()).toBe(1);

			await r.coordinator.shutdown();
			stopped = true;
			expect(await topicHighWatermark()).toBe(r.acceptedProviders.length);
		} finally {
			if (!stopped) await r.coordinator.shutdown();
			await r.client.quit();
		}
	}, 15_000);

	it('keeps provider health successful and authority uninitialized when candidate Kafka delivery fails', async () => {
		const disconnected = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
		await disconnected.connect();
		await disconnected.disconnect();

		const r = runtime(
			async () => ({ error: 'http_503' }),
			async () => ({
				kind: 'ok',
				messages: [message('opensky', 1)],
				responseTime: Math.floor(Date.now() / 1_000),
				creditsRemaining: 399,
			}),
			async (messages) => {
				const results = await disconnected.send({ topic, messages });
				return results[0]?.baseOffset ?? 'unknown';
			},
		);

		let stopped = false;
		try {
			r.coordinator.start();
			await waitFor(() =>
				r.logs.some((line) => line.startsWith('candidate publish failed: authority stays none')),
			);

			const authority = await redis.hgetall(authorityKey);
			expect(authority['provider']).toBeUndefined();
			expect(authority['epoch']).toBeUndefined();
			expect(await redis.hget(healthKeys.opensky, 'state')).toBe('RECOVERING');
			expect(await redis.hget(healthKeys.opensky, 'last_failure_ms')).toBe('');
			expect(await topicHighWatermark()).toBe(0);

			await r.coordinator.shutdown();
			stopped = true;
		} finally {
			if (!stopped) await r.coordinator.shutdown();
			await r.client.quit();
		}
	});

	it('restores a stored OpenSky authority conservatively and closes predecessor coverage before publishing', async () => {
		const before = Date.now() - 2_000;
		await redis.hset(
			authorityKey,
			'provider',
			'opensky',
			'epoch',
			'7',
			'authority_since_ms',
			String(before - 2_000),
			'coverage_open_since_ms',
			String(before - 1_000),
			'last_active_success_ms',
			String(before - 500),
			'timeline_version',
			'10',
		);
		await redis.hset(healthKeys.opensky, ...toHashFields('opensky', healthy('opensky', before)));
		await redis.hset(healthKeys.adsbfi, ...toHashFields('adsbfi', healthy('adsbfi', before)));

		let adsbSequence = 0;
		let openskySequence = 0;
		let responseNowMs = 1_790_600_000_000;
		const r = runtime(
			async () => ({
				messages: [message('adsbfi', ++adsbSequence)],
				responseNowMs: (responseNowMs += 1_000),
				total: 1,
				skippedNonIcao: 0,
				skippedNoPosition: 0,
				skippedOutsideBox: 0,
			}),
			async () => ({
				kind: 'ok',
				messages: [message('opensky', ++openskySequence)],
				responseTime: Math.floor(Date.now() / 1_000),
				creditsRemaining: 399,
			}),
		);

		let stopped = false;
		try {
			r.coordinator.start();

			await waitFor(async () => (await redis.zcard(coverageKey)) === 1);
			expect((await redis.zrange(coverageKey, '0', '-1'))[0]).toMatch(
				/^opensky\|\d+\|\d+\|coordinator_down$/,
			);
			await waitFor(() => r.acceptedProviders.length > 0);
			await waitFor(async () => (await redis.hget(authorityKey, 'coverage_open_since_ms')) !== '');

			expect(await redis.hget(authorityKey, 'provider')).toBe('opensky');
			expect(await redis.hget(authorityKey, 'epoch')).toBe('7');
			expect(r.acceptedProviders.every((p) => p === 'opensky')).toBe(true);

			await r.coordinator.shutdown();
			stopped = true;
			expect(await topicHighWatermark()).toBe(r.acceptedProviders.length);
		} finally {
			if (!stopped) await r.coordinator.shutdown();
			await r.client.quit();
		}
	});
});
