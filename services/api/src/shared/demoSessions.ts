// Shared counter for active demo WebSocket connections.
// Imported by both routes/auth.ts (cap check at token issue time)
// and ws/wsServer.ts (increment/decrement on connect/close).
// Module-level state is safe here: one Node process, one wsServer instance.
// MAX_DEMO_CONNECTIONS is owned by config.ts.

let activeCount = 0;

export function getDemoCount(): number {
	return activeCount;
}

export function incrementDemoCount(): void {
	activeCount++;
}

export function decrementDemoCount(): void {
	if (activeCount > 0) activeCount--;
}
