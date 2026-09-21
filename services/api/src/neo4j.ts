import neo4j from 'neo4j-driver';
import { config } from './config.js';

// Module-level driver, one per process -- same pattern as db.ts's pool and
// redis.ts's client. Unlike the correlation-worker's single long-lived
// session (a Kafka consumer, one logical stream of work for its whole
// process lifetime), an HTTP API opens a new session per request below,
// since concurrent requests must not share one session's internal
// serialization.
export const neo4jDriver = neo4j.driver(
	config.NEO4J_URI,
	neo4j.auth.basic(config.NEO4J_USER, config.NEO4J_PASSWORD),
);
