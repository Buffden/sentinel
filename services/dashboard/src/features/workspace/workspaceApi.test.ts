import { beforeEach, describe, expect, it, vi } from 'vitest'
import { fetchApi } from '@/features/auth/apiClient'
import { forceReconnect } from '@/shared/realtime/liveSocket'
import { getWorkspaceScope, saveWorkspaceScope, type WorkspaceScope } from './workspaceApi'

vi.mock('@/features/auth/apiClient', () => ({ fetchApi: vi.fn() }))
vi.mock('@/shared/realtime/liveSocket', () => ({ forceReconnect: vi.fn() }))

const mockFetchApi = vi.mocked(fetchApi)
const mockForceReconnect = vi.mocked(forceReconnect)

function jsonResponse(status: number, body: unknown): Response {
	return {
		status,
		ok: status >= 200 && status < 300,
		json: async () => body,
	} as Response
}

const SCOPE: WorkspaceScope = {
	geo_region: {
		name: 'France',
		bounds: { min_lat: 41.3, max_lat: 51.1, min_lon: -5.2, max_lon: 9.6 },
	},
	entity_types: ['aircraft'],
	alert_types: ['SIGNAL_LOSS'],
}

beforeEach(() => {
	mockFetchApi.mockReset()
	mockForceReconnect.mockReset()
})

describe('getWorkspaceScope', () => {
	it('returns the saved scope on 200', async () => {
		mockFetchApi.mockResolvedValue(jsonResponse(200, SCOPE))
		await expect(getWorkspaceScope()).resolves.toEqual(SCOPE)
	})

	it('returns null on 404 (no saved workspace yet)', async () => {
		mockFetchApi.mockResolvedValue(jsonResponse(404, { error: 'no_workspace' }))
		await expect(getWorkspaceScope()).resolves.toBeNull()
	})

	it('throws on an unexpected error status', async () => {
		mockFetchApi.mockResolvedValue(jsonResponse(500, {}))
		await expect(getWorkspaceScope()).rejects.toThrow()
	})
})

describe('saveWorkspaceScope', () => {
	it('saves and reconnects the live socket on success', async () => {
		mockFetchApi.mockResolvedValue(jsonResponse(200, SCOPE))

		const result = await saveWorkspaceScope(SCOPE)

		expect(result).toEqual(SCOPE)
		expect(mockForceReconnect).toHaveBeenCalledTimes(1)
	})

	it('does not reconnect when the save is rejected', async () => {
		mockFetchApi.mockResolvedValue(jsonResponse(400, { error: 'Invalid geo_region' }))

		await expect(saveWorkspaceScope(SCOPE)).rejects.toThrow()
		expect(mockForceReconnect).not.toHaveBeenCalled()
	})
})
