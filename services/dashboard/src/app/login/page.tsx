'use client'

import { useState } from 'react'
import { useRouter } from 'next/navigation'
import { GoogleLogin } from '@react-oauth/google'

const GOOGLE_CLIENT_ID = process.env['NEXT_PUBLIC_GOOGLE_CLIENT_ID'] ?? ''

export default function LoginPage() {
	const router = useRouter()
	const [error, setError] = useState<string | null>(null)
	const [demoLoading, setDemoLoading] = useState(false)

	async function handleGoogleSuccess(credentialResponse: { credential?: string }) {
		setError(null)
		const idToken = credentialResponse.credential
		if (!idToken) {
			setError('Google did not return a credential. Try again.')
			return
		}
		try {
			const res = await fetch('/api/auth/google', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ id_token: idToken }),
			})
			if (!res.ok) {
				const body = (await res.json()) as { error?: string }
				setError(body.error ?? 'Login failed.')
				return
			}
			router.replace('/')
		} catch {
			setError('Network error. Check the API is running.')
		}
	}

	async function handleDemo() {
		setError(null)
		setDemoLoading(true)
		try {
			const res = await fetch('/api/auth/demo', { method: 'POST' })
			if (res.status === 429) {
				const body = (await res.json()) as { error?: string }
				setError(body.error ?? 'Demo unavailable. Try again later.')
				return
			}
			if (!res.ok) {
				setError('Demo login failed.')
				return
			}
			router.replace('/')
		} catch {
			setError('Network error. Check the API is running.')
		} finally {
			setDemoLoading(false)
		}
	}

	return (
		<div
			style={{
				display: 'flex',
				height: '100vh',
				width: '100vw',
				alignItems: 'center',
				justifyContent: 'center',
				background: 'var(--color-bg-app)',
			}}
		>
			<div
				style={{
					display: 'flex',
					flexDirection: 'column',
					alignItems: 'center',
					gap: 'var(--space-6)',
					padding: 'var(--space-6)',
					background: 'var(--color-bg-panel)',
					border: '1px solid var(--color-border)',
					borderRadius: 'var(--panel-border-radius)',
					minWidth: 320,
				}}
			>
				{/* Wordmark */}
				<div style={{ textAlign: 'center' }}>
					<div
						style={{
							fontFamily: 'var(--font-mono)',
							fontSize: 22,
							fontWeight: 700,
							letterSpacing: '0.12em',
							color: 'var(--color-text-primary)',
							textTransform: 'uppercase',
						}}
					>
						Sentinel
					</div>
					<div
						style={{
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-xs)',
							color: 'var(--color-text-muted)',
							letterSpacing: '0.08em',
							marginTop: 4,
						}}
					>
						Real-time geospatial monitoring
					</div>
				</div>

				{/* Divider */}
				<div
					style={{ width: '100%', height: 1, background: 'var(--color-border-subtle)' }}
				/>

				{/* Google sign-in — only rendered when client ID is configured */}
				{GOOGLE_CLIENT_ID ? (
					<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 'var(--space-2)', width: '100%' }}>
						<GoogleLogin
							onSuccess={handleGoogleSuccess}
							onError={() => setError('Google sign-in failed. Try again.')}
							theme="filled_black"
							size="large"
							text="signin_with"
							shape="rectangular"
						/>
					</div>
				) : (
					<div
						style={{
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-xs)',
							color: 'var(--color-text-muted)',
							textAlign: 'center',
						}}
					>
						Google sign-in not configured.
						<br />
						Set NEXT_PUBLIC_GOOGLE_CLIENT_ID to enable.
					</div>
				)}

				{/* Divider */}
				<div
					style={{ width: '100%', height: 1, background: 'var(--color-border-subtle)' }}
				/>

				{/* Demo access */}
				<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 'var(--space-2)', width: '100%' }}>
					<button
						onClick={handleDemo}
						disabled={demoLoading}
						style={{
							width: '100%',
							padding: 'var(--space-2) var(--space-4)',
							background: 'transparent',
							borderWidth: 1,
							borderStyle: 'solid',
							borderColor: 'var(--color-border)',
							borderRadius: 'var(--panel-border-radius)',
							color: 'var(--color-text-secondary)',
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-sm)',
							fontWeight: 600,
							letterSpacing: '0.06em',
							cursor: demoLoading ? 'not-allowed' : 'pointer',
							opacity: demoLoading ? 0.6 : 1,
						}}
					>
						{demoLoading ? 'Starting demo…' : 'Try Demo (3 min)'}
					</button>
					<div
						style={{
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-xs)',
							color: 'var(--color-text-muted)',
							textAlign: 'center',
						}}
					>
						No account required. Session expires after 3 minutes.
					</div>
				</div>

				{/* Error message */}
				{error && (
					<div
						role="alert"
						style={{
							width: '100%',
							padding: 'var(--space-2) var(--space-3)',
							background: 'rgba(239,68,68,0.1)',
							borderWidth: 1,
							borderStyle: 'solid',
							borderColor: 'var(--color-status-critical)',
							borderRadius: 'var(--panel-border-radius)',
							color: 'var(--color-status-critical)',
							fontFamily: 'var(--font-mono)',
							fontSize: 'var(--font-size-xs)',
						}}
					>
						{error}
					</div>
				)}
			</div>
		</div>
	)
}
