import pg from 'pg';
import { config } from './config.js';

const { Pool } = pg;

export const pool = new Pool({
	connectionString: config.PG_URL,
	max: config.PG_POOL_MAX,
});

// Runs fn inside one BEGIN/COMMIT/ROLLBACK transaction on a single checked-out
// client. Advisory locks (pg_advisory_xact_lock) are scoped to the physical
// connection that took them, so a caller that mixed calls through the bare
// pool could have its lock silently apply to a different connection than the
// one running the statements it's meant to protect. Every statement in fn
// must run on the client it's given, never on pool directly.
export async function withTransaction<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
	const client = await pool.connect();
	try {
		await client.query('BEGIN');
		const result = await fn(client);
		await client.query('COMMIT');
		return result;
	} catch (err) {
		await client.query('ROLLBACK');
		throw err;
	} finally {
		client.release();
	}
}
