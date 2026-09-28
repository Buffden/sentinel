// Structured logging for the Alert Evaluator: one JSON object per line on
// stdout, in the log contract shared by every Sentinel service.
//
// No side effects beyond creating the logger: integration tests import
// evaluator.ts, which imports this. Process handlers live in processHandlers.ts,
// which only main.ts loads.

import { randomUUID } from 'node:crypto';
import type { logCreator } from 'kafkajs';
import pino from 'pino';

// One identity per process: the leader lease's owner value and every log
// line's instance_id, so a lease holder in Redis matches its logs.
export const instanceId = randomUUID();

// Every err is { type, message, stack }, whatever was thrown or rejected.
// type comes from err.name: Node's process warnings are plain Errors named
// e.g. TimeoutNegativeWarning, which the constructor name would hide. An
// object already in that shape (kafkaErr below) passes through; the standard
// serializer would relabel it type "Object". Any other value, such as a
// rejected string or null, becomes a NonError whose message is the value.
export function serializeErr(e: unknown) {
	if (e instanceof Error) return { ...pino.stdSerializers.err(e), type: e.name };
	if (isErrShape(e)) return e;
	return { type: 'NonError', message: describeValue(e), stack: '' };
}

function isErrShape(e: unknown): boolean {
	if (typeof e !== 'object' || e === null) return false;
	const { type, message, stack } = e as Record<string, unknown>;
	return typeof type === 'string' && typeof message === 'string' && typeof stack === 'string';
}

function describeValue(value: unknown): string {
	if (typeof value === 'string') return value;
	try {
		return JSON.stringify(value) ?? String(value);
	} catch {
		// Circular objects and BigInts cannot be JSON-encoded.
		return String(value);
	}
}

export const logger = pino(
	{
		base: { service: 'alert-evaluator', instance_id: instanceId },
		messageKey: 'msg',
		timestamp: () => `,"ts":"${new Date().toISOString()}"`,
		formatters: { level: (label) => ({ level: label }) },
		serializers: { err: serializeErr },
	},
	// Synchronous writes: a fatal line is on stdout before process.exit runs.
	pino.destination({ dest: 1, sync: true }),
);

export function log(
	level: 'info' | 'warn' | 'error',
	message: string,
	extra?: Record<string, unknown>,
): void {
	logger[level](extra ?? {}, message);
}

// ---- kafkajs ---------------------------------------------------------------

const KAFKA_LEVELS: Record<number, 'error' | 'warn' | 'info' | 'debug'> = {
	1: 'error',
	2: 'warn',
	4: 'info',
	5: 'debug',
};

const snakeCase = (key: string) => key.replace(/[A-Z]/g, (c) => `_${c.toLowerCase()}`);

// kafkajs logs an error as a message string plus a separate stack. The type
// and code come from the stack's first line, e.g. "AggregateError [ECONNREFUSED]: "
// or a bare "KafkaJSNonRetriableError".
function kafkaErr(error: unknown, stack: unknown) {
	if (error === undefined && stack === undefined) return undefined;
	const stackText = typeof stack === 'string' ? stack : '';
	const head = /^(\w+)(?: \[(\w+)\])?(?::\s?(.*))?$/.exec(stackText.split('\n')[0] ?? '');
	return {
		type: head?.[1] ?? 'Error',
		message: typeof error === 'string' ? error : (head?.[3] ?? ''),
		stack: stackText,
		...(head?.[2] ? { code: head[2] } : {}),
	};
}

// Routes kafkajs's own logs through pino in the contract's field names. The
// Kafka client's logLevel decides which levels reach here.
export const kafkaLogCreator: logCreator =
	() =>
	({ namespace, level, log: entry }) => {
		const {
			message,
			timestamp: _kafkaTs,
			logger: _kafkaLogger,
			error,
			stack,
			retryTime,
			...rest
		} = entry as Record<string, unknown> & { message: string };
		const fields: Record<string, unknown> = { component: 'kafkajs', namespace };
		for (const [key, value] of Object.entries(rest)) fields[snakeCase(key)] = value;
		if (retryTime !== undefined) fields.retry_time_ms = retryTime;
		const err = kafkaErr(error, stack);
		if (err) fields.err = err;
		logger[KAFKA_LEVELS[level] ?? 'warn'](fields, message);
	};
