import pg from 'pg';
import { config } from './config.js';

const { Pool } = pg;

export const pool = new Pool({
	connectionString: config.PG_URL,
	max: config.PG_POOL_MAX,
});
