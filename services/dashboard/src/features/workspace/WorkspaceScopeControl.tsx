'use client'

// CP5: top-nav entry point for the workspace scope editor. Demo sessions
// never see this at all -- they have no workspace to configure (CP1) --
// and the modal opens automatically, non-dismissibly, for an operator with
// no saved scope yet, per ADR-012's "no scope, no stream" rule.

import { useEffect, useState } from 'react'
import WorkspaceScopeModal from './WorkspaceScopeModal'
import {
	getRegions,
	getWorkspaceScope,
	type PredefinedRegion,
	type WorkspaceScope,
} from './workspaceApi'

type Role = 'operator' | 'demo' | 'unknown'

export default function WorkspaceScopeControl() {
	const [role, setRole] = useState<Role>('unknown')
	const [regions, setRegions] = useState<PredefinedRegion[]>([])
	const [scope, setScope] = useState<WorkspaceScope | null>(null)
	const [modalOpen, setModalOpen] = useState(false)
	const [firstTime, setFirstTime] = useState(false)

	useEffect(() => {
		fetch('/api/auth/me')
			.then((r) => (r.ok ? r.json() : null))
			.then((data: { role?: string } | null) => {
				if (data?.role !== 'operator') return
				setRole('operator')
				return Promise.all([getRegions(), getWorkspaceScope()]).then(
					([fetchedRegions, fetchedScope]) => {
						setRegions(fetchedRegions)
						setScope(fetchedScope)
						if (fetchedScope === null) {
							setFirstTime(true)
							setModalOpen(true)
						}
					},
				)
			})
			.catch(() => null)
	}, [])

	if (role !== 'operator') return null

	return (
		<>
			<button
				onClick={() => {
					setFirstTime(false)
					setModalOpen(true)
				}}
				title="Workspace scope"
				style={{
					background: 'transparent',
					border: 'none',
					padding: 0,
					cursor: 'pointer',
					display: 'flex',
					alignItems: 'center',
				}}
			>
				<svg width="20" height="20" viewBox="0 0 24 24" fill="none">
					<circle cx="12" cy="12" r="8" stroke="var(--color-status-info)" strokeWidth="1.5" />
					<path d="M6 12a6 6 0 0 1 12 0" stroke="var(--color-status-info)" strokeWidth="1.5" />
					<circle cx="12" cy="8" r="1.5" fill="var(--color-status-info)" />
				</svg>
			</button>

			{modalOpen && (
				<WorkspaceScopeModal
					regions={regions}
					initialScope={scope}
					firstTime={firstTime}
					onSaved={(saved) => {
						setScope(saved)
						setFirstTime(false)
						setModalOpen(false)
					}}
					onCancel={() => setModalOpen(false)}
				/>
			)}
		</>
	)
}
