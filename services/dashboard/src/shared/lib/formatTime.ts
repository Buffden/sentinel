// Formats a source-event-time (or any epoch ms) as "HH:MM UTC" for operator
// display. Always UTC — this is an operations tool, not a consumer app, and
// mixing local time zones across operators watching the same event would be
// confusing during incident review.
export function formatUtcTime(ms: number): string {
	const d = new Date(ms)
	const hh = String(d.getUTCHours()).padStart(2, '0')
	const mm = String(d.getUTCMinutes()).padStart(2, '0')
	return `${hh}:${mm} UTC`
}
