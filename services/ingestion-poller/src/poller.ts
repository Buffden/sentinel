// OpenSky ingestion poller.
//
// Polls the OpenSky Network REST API on a fixed interval and publishes one
// adsb.raw Kafka message per state vector, keyed by icao24.
//
// Responsibility boundary (ARCHITECTURE.md):
//   The poller may unwrap the provider response envelope and split it into
//   per-entity records. Field coercion, canonical naming, validation,
//   persistence, and DLQ handling belong to the Position Consumer.
//
// Field names follow OpenSky's own naming so the raw Kafka log faithfully
// represents the source. The Position Consumer maps them to the canonical
// schema.
//
// Message key: icao24 (ICAO 24-bit aircraft address).
//   This becomes entity_id in the canonical schema. Keying by icao24 ensures
//   that when adsb.raw is scaled to multiple partitions, all events for the
//   same aircraft land in the same partition and are consumed in arrival order.
//
// Rate limiting:
//   Anonymous OpenSky accounts are limited to approximately one request per
//   10 seconds. POLL_INTERVAL_MS defaults to 10 000. OPENSKY_LAMIN/LOMIN/
//   LAMAX/LOMAX scope the bounding box to reduce response size and credit
//   consumption. Defaults cover UK + Western Europe.

import { Kafka, Partitioners } from 'kafkajs';

// ---- Configuration ---------------------------------------------------------

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const TOPIC = 'adsb.raw';

// Poll interval in milliseconds. OpenSky recommends >= 10 s for anonymous use.
const POLL_INTERVAL_MS = Number(process.env['POLL_INTERVAL_MS'] ?? 10_000);

// Bounding box for OpenSky query. Defaults to UK + Western Europe.
const LAMIN = Number(process.env['OPENSKY_LAMIN'] ?? 49.0);
const LOMIN = Number(process.env['OPENSKY_LOMIN'] ?? -8.0);
const LAMAX = Number(process.env['OPENSKY_LAMAX'] ?? 61.0);
const LOMAX = Number(process.env['OPENSKY_LOMAX'] ?? 10.0);

const OPENSKY_URL =
  `https://opensky-network.org/api/states/all` +
  `?lamin=${LAMIN}&lomin=${LOMIN}&lamax=${LAMAX}&lomax=${LOMAX}`;

// Fetch timeout leaves headroom inside the poll interval.
const FETCH_TIMEOUT_MS = 8_000;

// ---- Types -----------------------------------------------------------------

// Provider-fidelity shape of one adsb.raw Kafka message.
// Field names follow OpenSky's own naming so the raw log faithfully represents
// the source. The Position Consumer maps these to the canonical schema.
//
// fetched_at_ms is an operational timestamp (processing time, not source event
// time). It documents when the poll cycle ran and is useful for measuring the
// lag between OpenSky's update interval and Kafka delivery. It is never used
// as a canonical event-time anchor — that role belongs to time_position.
interface AdsbRawEvent {
  icao24: string;
  callsign: string | null;
  origin_country: string;
  time_position: number | null; // Unix seconds; source event time for this position
  last_contact: number;         // Unix seconds; last time any transponder message arrived
  lon: number | null;
  lat: number | null;
  baro_altitude: number | null; // metres above mean sea level (barometric)
  on_ground: boolean;
  velocity: number | null;      // m/s ground speed
  true_track: number | null;    // degrees clockwise from north
  vertical_rate: number | null; // m/s; positive = climbing
  geo_altitude: number | null;  // metres; GNSS altitude; may differ from baro
  squawk: string | null;
  spi: boolean;
  position_source: number;      // 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
  fetched_at_ms: number;        // processing time of this poll cycle; NOT source event time
}

// ---- Kafka setup -----------------------------------------------------------

const kafka = new Kafka({
  clientId: 'ingestion-poller',
  brokers: BROKERS,
  logLevel: 0,
});

const producer = kafka.producer({
  // LegacyPartitioner preserves kafkajs v1 hash behaviour. Required in v2 to
  // suppress the deprecation warning. When adsb.raw is scaled to multiple
  // partitions, this partitioner hashes the icao24 key with murmur2 to route
  // all events for the same aircraft to the same partition.
  createPartitioner: Partitioners.LegacyPartitioner,
});

// ---- Logging ---------------------------------------------------------------

function log(
  level: 'info' | 'warn' | 'error',
  message: string,
  extra?: Record<string, unknown>,
): void {
  process.stdout.write(
    JSON.stringify({
      timestamp: new Date().toISOString(),
      level,
      service: 'ingestion-poller',
      message,
      ...extra,
    }) + '\n',
  );
}

// ---- OpenSky fetch ---------------------------------------------------------

// OpenSky returns state vectors as positional arrays. Index positions are fixed
// by the API contract and documented here to make the mapping auditable.
// Index 12 is the sensors array (receiver IDs) — not needed downstream, skipped.
function mapStateVector(state: unknown[], fetchedAtMs: number): AdsbRawEvent {
  return {
    icao24: state[0] as string,
    callsign: typeof state[1] === 'string' ? (state[1].trim() || null) : null,
    origin_country: state[2] as string,
    time_position: state[3] as number | null,
    last_contact: state[4] as number,
    lon: state[5] as number | null,
    lat: state[6] as number | null,
    baro_altitude: state[7] as number | null,
    on_ground: state[8] as boolean,
    velocity: state[9] as number | null,
    true_track: state[10] as number | null,
    vertical_rate: state[11] as number | null,
    // [12] sensors — skipped
    geo_altitude: state[13] as number | null,
    squawk: state[14] as string | null,
    spi: state[15] as boolean,
    position_source: state[16] as number,
    fetched_at_ms: fetchedAtMs,
  };
}

async function fetchStateVectors(): Promise<AdsbRawEvent[]> {
  const fetchedAtMs = Date.now();

  const response = await fetch(OPENSKY_URL, {
    headers: { Accept: 'application/json' },
    signal: AbortSignal.timeout(FETCH_TIMEOUT_MS),
  });

  if (!response.ok) {
    throw new Error(`OpenSky returned HTTP ${response.status}`);
  }

  const body = (await response.json()) as {
    time: number;
    states: unknown[] | null;
  };

  if (!Array.isArray(body.states) || body.states.length === 0) {
    return [];
  }

  return (body.states as unknown[][]).map((s) => mapStateVector(s, fetchedAtMs));
}

// ---- Poll cycle ------------------------------------------------------------

async function pollOnce(): Promise<void> {
  let events: AdsbRawEvent[];

  try {
    events = await fetchStateVectors();
  } catch (err) {
    // Log and skip this cycle. A transient OpenSky outage or rate-limit
    // response should not crash the poller — the next cycle will retry.
    log('warn', 'opensky fetch failed, skipping cycle', {
      error: err instanceof Error ? err.message : String(err),
    });
    return;
  }

  if (events.length === 0) {
    log('info', 'opensky returned no state vectors');
    return;
  }

  const messages = events.map((event) => ({
    key: event.icao24,
    value: JSON.stringify(event),
  }));

  const results = await producer.send({ topic: TOPIC, messages });

  // With one partition, results has exactly one RecordMetadata entry.
  // Log the base offset so you can verify the batch with:
  //   docker exec sentinel-redpanda rpk topic consume adsb.raw --offset <N> --num 1
  const firstOffset = results[0]?.baseOffset ?? 'unknown';

  log('info', 'poll cycle complete', {
    state_vectors: events.length,
    topic: TOPIC,
    first_offset: firstOffset,
  });
}

// ---- Poll loop -------------------------------------------------------------

let pollTimeout: ReturnType<typeof setTimeout> | null = null;
let stopping = false;

function scheduleNextPoll(): void {
  if (stopping) return;
  pollTimeout = setTimeout(() => {
    void pollOnce()
      .catch((err: unknown) => {
        log('error', 'poll cycle error', {
          error: err instanceof Error ? err.message : String(err),
        });
      })
      .finally(() => scheduleNextPoll());
  }, POLL_INTERVAL_MS);
}

// ---- Entry point -----------------------------------------------------------

async function run(): Promise<void> {
  log('info', 'producer connecting', { brokers: BROKERS });
  await producer.connect();
  log('info', 'producer connected');

  log('info', 'poller starting', {
    url: OPENSKY_URL,
    poll_interval_ms: POLL_INTERVAL_MS,
  });

  // Run the first poll immediately so you see output without waiting a full
  // interval. Subsequent polls are spaced POLL_INTERVAL_MS apart.
  await pollOnce().catch((err: unknown) => {
    log('error', 'initial poll error', {
      error: err instanceof Error ? err.message : String(err),
    });
  });

  scheduleNextPoll();
}

async function shutdown(signal: string): Promise<void> {
  stopping = true;
  if (pollTimeout !== null) clearTimeout(pollTimeout);
  log('info', 'shutdown initiated', { signal });
  await producer.disconnect();
  log('info', 'producer disconnected');
  process.exit(0);
}

process.on('SIGINT', () => {
  shutdown('SIGINT').catch(() => process.exit(1));
});
process.on('SIGTERM', () => {
  shutdown('SIGTERM').catch(() => process.exit(1));
});

run().catch((err: unknown) => {
  log('error', 'poller failed', {
    error: {
      name: err instanceof Error ? err.name : 'UnknownError',
      message: err instanceof Error ? err.message : String(err),
    },
  });
  process.exit(1);
});
