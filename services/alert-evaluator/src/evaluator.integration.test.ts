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
import { afterAll, afterEach, beforeAll, describe, expect, it } from 'vitest';
import { config } from './config.js';
import { producer, redis, runScan } from './evaluator.js';

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
	fields: Partial<{ last_seen_ms: number; on_ground: boolean; entity_type: string }>,
): Promise<number> {
	const hashFields: string[] = ['entity_type', fields.entity_type ?? 'aircraft'];
	if (fields.last_seen_ms !== undefined) {
		hashFields.push('last_seen_ms', String(fields.last_seen_ms));
	}
	if (fields.on_ground !== undefined) {
		hashFields.push('on_ground', String(fields.on_ground));
	}
	return redis.hset(`entity:live:${entityId}`, ...hashFields);
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
		const entityId = `test-evaluator-${randomUUID()}`;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 10_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs });

		try {
			await runScan();

			const alert = await waitForAlert(entityId);
			expect(alert).toBeDefined();
			expect(alert?.alert_id).toBe(`${entityId}:SIGNAL_LOSS:${darkSinceMs}`);
			expect(alert?.alert_type).toBe('SIGNAL_LOSS');
			expect(alert?.payload['dark_since_ms']).toBe(darkSinceMs);

			const gate = await redis.hgetall(`alert-state:${entityId}`);
			expect(gate['dark_since_ms']).toBe(String(darkSinceMs));
			expect(gate['signal_loss_alert_id']).toBe(alert?.alert_id);
		} finally {
			await redis.del(`entity:live:${entityId}`, `alert-state:${entityId}`);
		}
	});

	it('does not re-alert on a second scan tick for the same episode', async () => {
		const entityId = `test-evaluator-${randomUUID()}`;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 10_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs });

		try {
			await runScan();
			const first = await waitForAlert(entityId);
			expect(first).toBeDefined();

			// Same episode, second tick: the entity is still dark and the gate
			// from the first tick is still set.
			receivedAlerts.length = 0;
			await runScan();
			await assertNoAlert(entityId);
		} finally {
			await redis.del(`entity:live:${entityId}`, `alert-state:${entityId}`);
		}
	});

	it('computes a distinct alert_id for a new episode after the gate is cleared', async () => {
		const entityId = `test-evaluator-${randomUUID()}`;
		const firstDarkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 20_000;
		await seedLiveEntity(entityId, { last_seen_ms: firstDarkSinceMs });

		try {
			await runScan();
			const first = await waitForAlert(entityId);
			expect(first).toBeDefined();

			// Simulate what the Position Consumer does when the entity resumes
			// transmitting: clear the episode gate. Then the entity goes dark
			// again at a later source timestamp — a genuinely new episode.
			await redis.del(`alert-state:${entityId}`);
			receivedAlerts.length = 0;
			const secondDarkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 5_000;
			await seedLiveEntity(entityId, { last_seen_ms: secondDarkSinceMs });

			await runScan();
			const second = await waitForAlert(entityId);
			expect(second).toBeDefined();
			expect(second?.alert_id).not.toBe(first?.alert_id);
			expect(second?.alert_id).toBe(`${entityId}:SIGNAL_LOSS:${secondDarkSinceMs}`);
		} finally {
			await redis.del(`entity:live:${entityId}`, `alert-state:${entityId}`);
		}
	});

	it('does not alert an entity that is on the ground', async () => {
		const entityId = `test-evaluator-${randomUUID()}`;
		const darkSinceMs = Date.now() - config.SIGNAL_LOSS_THRESHOLD_MS - 10_000;
		await seedLiveEntity(entityId, { last_seen_ms: darkSinceMs, on_ground: true });

		try {
			await runScan();
			await assertNoAlert(entityId);
			expect(await redis.exists(`alert-state:${entityId}`)).toBe(0);
		} finally {
			await redis.del(`entity:live:${entityId}`, `alert-state:${entityId}`);
		}
	});

	it('does not alert an entity with no accepted position (missing last_seen_ms)', async () => {
		const entityId = `test-evaluator-${randomUUID()}`;
		await seedLiveEntity(entityId, {});

		try {
			await runScan();
			await assertNoAlert(entityId);
		} finally {
			await redis.del(`entity:live:${entityId}`, `alert-state:${entityId}`);
		}
	});

	it('does not alert an entity that is still within the silence threshold', async () => {
		const entityId = `test-evaluator-${randomUUID()}`;
		const recentMs = Date.now() - Math.floor(config.SIGNAL_LOSS_THRESHOLD_MS / 2);
		await seedLiveEntity(entityId, { last_seen_ms: recentMs });

		try {
			await runScan();
			await assertNoAlert(entityId);
			expect(await redis.exists(`alert-state:${entityId}`)).toBe(0);
		} finally {
			await redis.del(`entity:live:${entityId}`, `alert-state:${entityId}`);
		}
	});
});
