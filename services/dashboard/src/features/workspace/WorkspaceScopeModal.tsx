'use client'

// CP5: the scope editor from the approved mockup
// (concepts/workspace-scope-controls/mockups/workspace-scope-editor.svg).
// Region picker (predefined or custom bounds) + entity types (locked to
// aircraft in v1) + alert types (ROUTE_DEVIATION shown but disabled, since
// Phase 04 is deferred and nothing produces it yet). Saving calls
// saveWorkspaceScope, which itself reconnects the live WebSocket (CP4) --
// this component does not touch the connection directly.
//
// Checkboxes and the region select are custom-rendered rather than native
// <input>/<select> elements: native controls render with the OS's own
// light-mode widget chrome (a filled blue circle for a checked checkbox in
// Safari, an OS-styled dropdown arrow), which reads as a jarring light
// element against this dashboard's dark theme -- not a Sentinel design
// choice, just what the browser draws by default.

import { useState } from 'react'
import {
	saveWorkspaceScope,
	type GeoBounds,
	type PredefinedRegion,
	type WorkspaceScope,
} from './workspaceApi'

const ALERT_TYPES = [
	{ value: 'SIGNAL_LOSS', label: 'Signal Loss', color: 'var(--color-status-critical)' },
	{
		value: 'UNSCHEDULED_PROXIMITY',
		label: 'Unscheduled Proximity',
		color: 'var(--color-status-warning)',
	},
	{ value: 'COMPOSITE', label: 'Composite', color: 'var(--color-status-warning)' },
] as const

const CUSTOM_REGION = '__custom__'

const EMPTY_BOUNDS: GeoBounds = { min_lat: 0, max_lat: 0, min_lon: 0, max_lon: 0 }

const inputStyle: React.CSSProperties = {
	width: '100%',
	padding: '9px 10px',
	background: 'var(--color-bg-app)',
	border: '1px solid var(--color-border)',
	borderRadius: 6,
	color: 'var(--color-text-primary)',
	fontFamily: 'var(--font-mono)',
	fontSize: 'var(--font-size-sm)',
	outline: 'none',
}

const sectionLabelStyle: React.CSSProperties = {
	display: 'block',
	fontSize: 10,
	color: 'var(--color-text-muted)',
	textTransform: 'uppercase',
	letterSpacing: '0.1em',
	fontWeight: 700,
	marginBottom: 10,
}

interface CheckboxProps {
	checked: boolean
	disabled?: boolean
	accent: string
	onChange?: () => void
}

function Checkbox({ checked, disabled, accent, onChange }: CheckboxProps) {
	return (
		<button
			type="button"
			role="checkbox"
			aria-checked={checked}
			disabled={disabled}
			onClick={onChange}
			style={{
				width: 17,
				height: 17,
				borderRadius: 5,
				border: `1.5px solid ${checked ? accent : 'var(--color-border)'}`,
				background: checked ? accent : 'var(--color-bg-app)',
				display: 'flex',
				alignItems: 'center',
				justifyContent: 'center',
				cursor: disabled ? 'default' : 'pointer',
				opacity: disabled && !checked ? 0.45 : 1,
				padding: 0,
				flexShrink: 0,
				transition: 'background 120ms ease, border-color 120ms ease',
			}}
		>
			{checked && (
				<svg width="10" height="10" viewBox="0 0 10 10" fill="none">
					<path
						d="M2 5.2 L4.1 7.3 L8 2.8"
						stroke="var(--color-bg-app)"
						strokeWidth="1.8"
						strokeLinecap="round"
						strokeLinejoin="round"
					/>
				</svg>
			)}
		</button>
	)
}

interface WorkspaceScopeModalProps {
	regions: PredefinedRegion[]
	initialScope: WorkspaceScope | null
	firstTime: boolean
	onSaved: (scope: WorkspaceScope) => void
	onCancel: () => void
}

function isValidBounds(b: GeoBounds): boolean {
	return (
		Number.isFinite(b.min_lat) &&
		Number.isFinite(b.max_lat) &&
		Number.isFinite(b.min_lon) &&
		Number.isFinite(b.max_lon) &&
		b.min_lat < b.max_lat &&
		b.min_lon < b.max_lon
	)
}

export default function WorkspaceScopeModal({
	regions,
	initialScope,
	firstTime,
	onSaved,
	onCancel,
}: WorkspaceScopeModalProps) {
	const initialIsCustom = initialScope !== null && initialScope.geo_region.name === null
	const [regionName, setRegionName] = useState<string>(
		initialIsCustom
			? CUSTOM_REGION
			: (initialScope?.geo_region.name ?? regions[0]?.name ?? CUSTOM_REGION),
	)
	const [customBounds, setCustomBounds] = useState<GeoBounds>(
		initialIsCustom ? initialScope!.geo_region.bounds : EMPTY_BOUNDS,
	)
	const [alertTypes, setAlertTypes] = useState<Set<string>>(
		new Set(initialScope?.alert_types ?? []),
	)
	const [saving, setSaving] = useState(false)
	const [error, setError] = useState<string | null>(null)

	const bounds =
		regionName === CUSTOM_REGION
			? customBounds
			: (regions.find((r) => r.name === regionName)?.bounds ?? EMPTY_BOUNDS)
	const boundsValid = regionName !== CUSTOM_REGION || isValidBounds(customBounds)
	const canSave = boundsValid && alertTypes.size > 0 && !saving

	function toggleAlertType(value: string): void {
		setAlertTypes((prev) => {
			const next = new Set(prev)
			if (next.has(value)) next.delete(value)
			else next.add(value)
			return next
		})
	}

	async function handleSave(): Promise<void> {
		setSaving(true)
		setError(null)
		try {
			const scope: WorkspaceScope = {
				geo_region: { name: regionName === CUSTOM_REGION ? null : regionName, bounds },
				entity_types: ['aircraft'],
				alert_types: Array.from(alertTypes),
			}
			const saved = await saveWorkspaceScope(scope)
			onSaved(saved)
		} catch {
			setError('Could not save workspace scope. Check your bounds and try again.')
		} finally {
			setSaving(false)
		}
	}

	return (
		<div
			style={{
				position: 'fixed',
				inset: 0,
				top: 'var(--topnav-height)',
				background: 'rgba(0, 0, 0, 0.65)',
				backdropFilter: 'blur(1px)',
				display: 'flex',
				alignItems: 'flex-start',
				justifyContent: 'center',
				paddingTop: 90,
				zIndex: 900,
			}}
		>
			<div
				style={{
					width: 460,
					maxHeight: 'calc(100vh - 140px)',
					overflowY: 'auto',
					background: 'var(--color-bg-elevated)',
					border: '1px solid #333333',
					borderRadius: 10,
					fontFamily: 'var(--font-mono)',
					boxShadow: '0 24px 64px rgba(0, 0, 0, 0.6), 0 0 0 1px rgba(255, 255, 255, 0.03)',
				}}
			>
				<div
					style={{
						height: 44,
						display: 'flex',
						alignItems: 'center',
						justifyContent: 'space-between',
						padding: '0 var(--space-4)',
						borderBottom: '1px solid #2f2f2f',
					}}
				>
					<span
						style={{
							fontSize: 12,
							color: 'var(--color-text-primary)',
							textTransform: 'uppercase',
							letterSpacing: '0.1em',
							fontWeight: 700,
						}}
					>
						Workspace Scope
					</span>
					{!firstTime && (
						<button
							onClick={onCancel}
							aria-label="Close"
							style={{
								background: 'none',
								border: 'none',
								color: 'var(--color-text-muted)',
								cursor: 'pointer',
								fontSize: 16,
								lineHeight: 1,
								padding: 4,
								borderRadius: 4,
							}}
						>
							✕
						</button>
					)}
				</div>

				<div style={{ padding: '20px var(--space-4) var(--space-4)' }}>
					{firstTime && (
						<div
							style={{
								marginBottom: 20,
								padding: '10px 12px',
								borderRadius: 6,
								background: 'rgba(245, 158, 11, 0.08)',
								border: '1px solid rgba(245, 158, 11, 0.35)',
								color: '#fbbf7a',
								fontSize: 'var(--font-size-sm)',
								lineHeight: 1.4,
							}}
						>
							No workspace saved yet — you will not receive alerts until you save one.
						</div>
					)}

					<label style={sectionLabelStyle}>Region</label>
					<div style={{ position: 'relative' }}>
						<select
							value={regionName}
							onChange={(e) => setRegionName(e.target.value)}
							style={{
								...inputStyle,
								appearance: 'none',
								WebkitAppearance: 'none',
								paddingRight: 32,
								fontSize: 'var(--font-size-base)',
								cursor: 'pointer',
							}}
						>
							{regions.map((r) => (
								<option key={r.name} value={r.name}>
									{r.name}
								</option>
							))}
							<option value={CUSTOM_REGION}>Custom bounds…</option>
						</select>
						<svg
							width="12"
							height="12"
							viewBox="0 0 12 12"
							fill="none"
							style={{
								position: 'absolute',
								right: 12,
								top: '50%',
								transform: 'translateY(-50%)',
								pointerEvents: 'none',
							}}
						>
							<path
								d="M2.5 4.5 L6 8 L9.5 4.5"
								stroke="var(--color-text-secondary)"
								strokeWidth="1.5"
								strokeLinecap="round"
								strokeLinejoin="round"
							/>
						</svg>
					</div>

					{regionName === CUSTOM_REGION && (
						<div
							style={{
								display: 'grid',
								gridTemplateColumns: 'repeat(4, 1fr)',
								gap: 8,
								marginTop: 10,
							}}
						>
							{(['min_lat', 'max_lat', 'min_lon', 'max_lon'] as const).map((field) => (
								<div key={field}>
									<span
										style={{
											display: 'block',
											fontSize: 9,
											color: 'var(--color-text-muted)',
											marginBottom: 4,
											textTransform: 'uppercase',
											letterSpacing: '0.05em',
										}}
									>
										{field}
									</span>
									<input
										type="number"
										value={customBounds[field]}
										onChange={(e) =>
											setCustomBounds((prev) => ({
												...prev,
												[field]: parseFloat(e.target.value),
											}))
										}
										style={{ ...inputStyle, padding: '7px 8px', fontSize: 12 }}
									/>
								</div>
							))}
						</div>
					)}

					<div style={{ borderTop: '1px solid #262626', margin: '20px 0' }} />

					<label style={sectionLabelStyle}>Entity Types</label>
					<div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
						<Checkbox checked disabled accent="var(--color-status-info)" />
						<span style={{ color: 'var(--color-text-primary)', fontSize: 'var(--font-size-base)' }}>
							Aircraft
						</span>
						<span style={{ color: 'var(--color-text-muted)', fontSize: 11 }}>
							only tracked type in v1
						</span>
					</div>

					<div style={{ borderTop: '1px solid #262626', margin: '20px 0' }} />

					<label style={sectionLabelStyle}>Alert Types</label>
					<div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
						{ALERT_TYPES.map((t) => (
							<div
								key={t.value}
								onClick={() => toggleAlertType(t.value)}
								style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer' }}
							>
								{/* No onChange here: the wrapping div's onClick is the single
									toggle handler. This button's own click still bubbles up to
									it, so passing onChange too would fire toggleAlertType twice
									per click (on then immediately back off, net no-op). */}
								<Checkbox checked={alertTypes.has(t.value)} accent={t.color} />
								<span
									style={{ color: 'var(--color-text-primary)', fontSize: 'var(--font-size-base)' }}
								>
									{t.label}
								</span>
							</div>
						))}
						<div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
							<Checkbox checked={false} disabled accent="var(--color-text-muted)" />
							<span style={{ color: 'var(--color-text-muted)', fontSize: 'var(--font-size-base)' }}>
								Route Deviation
							</span>
							<span style={{ color: 'var(--color-text-muted)', fontSize: 11 }}>
								Phase 4 pending
							</span>
						</div>
					</div>

					{error && (
						<div
							style={{
								marginTop: 16,
								padding: '8px 10px',
								borderRadius: 6,
								background: 'rgba(239, 68, 68, 0.08)',
								border: '1px solid rgba(239, 68, 68, 0.35)',
								color: '#f87171',
								fontSize: 'var(--font-size-sm)',
							}}
						>
							{error}
						</div>
					)}

					<div style={{ display: 'flex', justifyContent: 'flex-end', gap: 10, marginTop: 24 }}>
						{!firstTime && (
							<button
								onClick={onCancel}
								disabled={saving}
								style={{
									padding: '9px 18px',
									background: 'transparent',
									border: '1px solid var(--color-border)',
									borderRadius: 6,
									color: 'var(--color-text-secondary)',
									fontFamily: 'var(--font-mono)',
									fontSize: 'var(--font-size-sm)',
									cursor: saving ? 'default' : 'pointer',
								}}
							>
								Cancel
							</button>
						)}
						<button
							onClick={handleSave}
							disabled={!canSave}
							style={{
								padding: '9px 20px',
								background: canSave ? 'var(--color-status-info)' : 'var(--color-bg-app)',
								border: `1px solid ${canSave ? 'var(--color-status-info)' : 'var(--color-border)'}`,
								borderRadius: 6,
								color: canSave ? '#ffffff' : 'var(--color-text-muted)',
								fontFamily: 'var(--font-mono)',
								fontSize: 'var(--font-size-sm)',
								fontWeight: 700,
								cursor: canSave ? 'pointer' : 'default',
								boxShadow: canSave ? '0 4px 14px rgba(59, 130, 246, 0.35)' : 'none',
								transition: 'box-shadow 120ms ease',
							}}
						>
							{saving ? 'Saving…' : 'Save'}
						</button>
					</div>
				</div>
			</div>
		</div>
	)
}
