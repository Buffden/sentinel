// Process-level warning and crash handlers. Imported first by main.ts and
// never by modules tests import: a crash handler that calls process.exit must
// not run inside a test worker.
//
// Being the first import, this module is evaluated before any dependency, so
// the handlers exist before a dependency's warning can be delivered. The
// launcher runs with --no-warnings, which turns off Node's plain-text warning
// printer; the 'warning' handler here is what keeps those warnings recorded.

import { logger, serializeErr } from './logger.js';

process.on('warning', (warning) => {
	logger.warn({ err: warning }, 'node process warning');
});

// Replaces Node's default crash printout, keeping its exit status.
function exitFatal(kind: string) {
	return (err: unknown) => {
		// Serialized here, not left to pino: pino drops an undefined value before
		// its serializers run, so a rejection with no reason would log no err.
		logger.fatal({ err: serializeErr(err), kind }, 'process terminating');
		process.exit(1);
	};
}
process.on('uncaughtException', exitFatal('uncaughtException'));
process.on('unhandledRejection', exitFatal('unhandledRejection'));
