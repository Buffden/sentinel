import { Redis } from 'ioredis';
import { config } from './config.js';
import { log } from './logger.js';

export const redis = new Redis(config.REDIS_URL);

// ioredis prints an 'error' event to stderr as plain text when nothing listens,
// once per failed reconnect during an outage. It reconnects on its own either
// way; the listener only keeps those errors in the log contract.
redis.on('error', (err) => {
	log('warn', 'redis client error', { connection: 'commands', err });
});
