'use client'

import WidgetHeader from '@/shared/ui/WidgetHeader'

const SAMPLE_FLIGHT = {
	callsign: 'UAE204',
	registration: 'A6-EUB',
	type: 'B77W',
	origin: 'DXB',
	destination: 'LHR',
	altitude: '38,000 ft',
	speed: '487 kts',
	heading: '312°',
	status: 'SIGNAL LOST',
	lastSeen: '14:32:18 UTC',
	lat: '48.2124° N',
	lon: '16.3743° E',
}

function Row({ label, value, highlight }: { label: string; value: string; highlight?: string }) {
	return (
		<div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: 'var(--space-1) var(--space-3)', borderBottom: '1px solid var(--color-border-subtle)' }}>
			<span style={{ fontSize: 'var(--font-size-xs)', color: 'var(--color-text-muted)', fontFamily: 'var(--font-mono)' }}>
				{label}
			</span>
			<span style={{ fontSize: 'var(--font-size-xs)', color: highlight ?? 'var(--color-text-primary)', fontFamily: 'var(--font-mono)', fontWeight: highlight ? 600 : 400 }}>
				{value}
			</span>
		</div>
	)
}

export default function FlightInfoWidget() {
	return (
		<div style={{ height: '100%', display: 'flex', flexDirection: 'column', background: 'var(--color-bg-panel)', overflow: 'hidden' }}>
			<WidgetHeader
				title={SAMPLE_FLIGHT.callsign}
				meta={`${SAMPLE_FLIGHT.type} · ${SAMPLE_FLIGHT.registration}`}
			/>

			<div style={{ flex: 1, overflowY: 'auto' }}>
				<Row label="STATUS"   value={SAMPLE_FLIGHT.status}      highlight="var(--color-status-critical)" />
				<Row label="LAST SEEN" value={SAMPLE_FLIGHT.lastSeen} />
				<Row label="ORIGIN"   value={SAMPLE_FLIGHT.origin} />
				<Row label="DEST"     value={SAMPLE_FLIGHT.destination} />
				<Row label="ALTITUDE" value={SAMPLE_FLIGHT.altitude} />
				<Row label="SPEED"    value={SAMPLE_FLIGHT.speed} />
				<Row label="HEADING"  value={SAMPLE_FLIGHT.heading} />
				<Row label="LAT"      value={SAMPLE_FLIGHT.lat} />
				<Row label="LON"      value={SAMPLE_FLIGHT.lon} />
			</div>
		</div>
	)
}
