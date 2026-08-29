import pg from 'pg';

const { Pool } = pg;

export const pool = new Pool({
  connectionString: process.env['PG_URL'] ?? 'postgres://sentinel:sentinel@localhost:5432/sentinel',
});
