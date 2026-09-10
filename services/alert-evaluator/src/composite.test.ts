// Unit tests for buildCompositeAlert (CP4) -- pure, no Redis/Kafka, so this
// runs without any infrastructure, unlike composite.integration.test.ts.
import { describe, expect, it } from 'vitest';
import { buildCompositeAlert } from './composite.js';
import type { CompositeCandidateDecision } from './composite.js';
import type { ProximityCandidateMessage } from './evaluator.js';

const candidate: ProximityCandidateMessage = {
	pair_key: 'entity-a:entity-b',
	entity_a_id: 'entity-a',
	entity_b_id: 'entity-b',
	episode_start_ms: 1_700_000_030_000,
	lat: 51.5,
	lon: -0.1,
	distance_at_detection: 42.5,
};

const activeDecision: CompositeCandidateDecision = {
	decision: 'COMPOSITE',
	candidate_id: `${candidate.pair_key}:${candidate.episode_start_ms}`,
	selected_entity_id: 'entity-a',
	loss_source: 'ACTIVE',
	dark_since_ms: 1_700_000_000_000,
	signal_loss_alert_id: 'entity-a:SIGNAL_LOSS:1700000000000',
	resumed_at_ms: null,
};

describe('buildCompositeAlert', () => {
	it('builds a deterministic COMPOSITE alert with the literal nested payload shape', () => {
		const alert = buildCompositeAlert(
			activeDecision,
			candidate,
			'aircraft',
			1_700_000_031_000,
			120_000,
		);

		expect(alert).toEqual({
			alert_id: 'entity-a:entity-b:COMPOSITE:1700000000000',
			entity_id: 'entity-a',
			counterparty_entity_id: 'entity-b',
			entity_type: 'aircraft',
			alert_type: 'COMPOSITE',
			priority: 'ELEVATED',
			status: 'NEW',
			detected_at_ms: 1_700_000_031_000,
			payload: {
				signal_loss: {
					dark_since_ms: 1_700_000_000_000,
					loss_source: 'ACTIVE',
					resumed_at_ms: null,
					signal_loss_alert_id: 'entity-a:SIGNAL_LOSS:1700000000000',
				},
				proximity: {
					pair_key: 'entity-a:entity-b',
					entity_a_id: 'entity-a',
					entity_b_id: 'entity-b',
					lat: 51.5,
					lon: -0.1,
					distance_metres: 42.5,
					episode_start_ms: 1_700_000_030_000,
				},
				correlation_window_ms: 120_000,
				supersedes_alert_ids: ['entity-a:SIGNAL_LOSS:1700000000000'],
			},
		});
	});

	it('is deterministic: identical explicit inputs produce byte-for-byte identical output', () => {
		const first = buildCompositeAlert(
			activeDecision,
			candidate,
			'aircraft',
			1_700_000_031_000,
			120_000,
		);
		const second = buildCompositeAlert(
			activeDecision,
			candidate,
			'aircraft',
			1_700_000_031_000,
			120_000,
		);

		expect(JSON.stringify(first)).toBe(JSON.stringify(second));
	});

	it('sets entity_id to selected_entity_id and counterparty to the other pair member, regardless of which member qualified', () => {
		const decisionOnB: CompositeCandidateDecision = {
			...activeDecision,
			selected_entity_id: 'entity-b',
			signal_loss_alert_id: 'entity-b:SIGNAL_LOSS:1700000000000',
		};

		const alert = buildCompositeAlert(decisionOnB, candidate, 'vessel', 1_700_000_031_000, 120_000);

		expect(alert.entity_id).toBe('entity-b');
		expect(alert.counterparty_entity_id).toBe('entity-a');
		expect(alert.payload.supersedes_alert_ids).toEqual(['entity-b:SIGNAL_LOSS:1700000000000']);
	});

	it('carries RECENT loss_source and resumed_at_ms through unchanged', () => {
		const recentDecision: CompositeCandidateDecision = {
			...activeDecision,
			loss_source: 'RECENT',
			resumed_at_ms: 1_700_000_010_000,
		};

		const alert = buildCompositeAlert(
			recentDecision,
			candidate,
			'aircraft',
			1_700_000_031_000,
			120_000,
		);

		expect(alert.payload.signal_loss.loss_source).toBe('RECENT');
		expect(alert.payload.signal_loss.resumed_at_ms).toBe(1_700_000_010_000);
	});

	it("sets priority to ELEVATED, per DATA_MODEL.md's priority-by-alert_type mapping (US-06: correlated evidence reads as one elevated incident)", () => {
		const alert = buildCompositeAlert(
			activeDecision,
			candidate,
			'aircraft',
			1_700_000_031_000,
			120_000,
		);

		expect(alert.priority).toBe('ELEVATED');
	});

	it('derives alert_id from pair_key and the decision dark_since_ms, not the candidate episode_start_ms', () => {
		const alert = buildCompositeAlert(
			activeDecision,
			candidate,
			'aircraft',
			1_700_000_031_000,
			120_000,
		);

		expect(alert.alert_id).toBe(`${candidate.pair_key}:COMPOSITE:${activeDecision.dark_since_ms}`);
		expect(alert.alert_id).not.toContain(String(candidate.episode_start_ms));
	});

	it('throws when the decision entity is not a member of the candidate pair', () => {
		const mismatched: CompositeCandidateDecision = {
			...activeDecision,
			selected_entity_id: 'entity-z',
		};

		expect(() =>
			buildCompositeAlert(mismatched, candidate, 'aircraft', 1_700_000_031_000, 120_000),
		).toThrow(/is not a member of/);
	});
});
