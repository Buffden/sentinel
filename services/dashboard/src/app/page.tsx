'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { GoogleLogin } from '@react-oauth/google'
import Footer from '@/shell/Footer'
import TopNav from '@/widgets/top-nav/TopNav'
import NavUserIcons from '@/shared/ui/NavUserIcons'

const GOOGLE_CLIENT_ID = process.env['NEXT_PUBLIC_GOOGLE_CLIENT_ID'] ?? ''

type AuthState = 'checking' | 'authenticated' | 'unauthenticated'

export default function LandingPage() {
	const router = useRouter()
	const [authState, setAuthState] = useState<AuthState>('checking')
	const [demoLoading, setDemoLoading] = useState(false)
	const [error, setError] = useState<string | null>(null)

	useEffect(() => {
		fetch('/api/healthz-auth')
			.then((res) => setAuthState(res.ok ? 'authenticated' : 'unauthenticated'))
			.catch(() => setAuthState('unauthenticated'))
	}, [])

	async function handleGoogleSuccess(credentialResponse: { credential?: string }) {
		setError(null)
		const idToken = credentialResponse.credential
		if (!idToken) {
			setError('Google did not return a credential.')
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
			router.push('/dashboard')
		} catch {
			setError('Network error.')
		}
	}

	async function handleDemo() {
		setError(null)
		setDemoLoading(true)
		try {
			const res = await fetch('/api/auth/demo', { method: 'POST' })
			if (res.status === 429) {
				const body = (await res.json()) as { error?: string }
				setError(body.error ?? 'Demo unavailable.')
				return
			}
			if (!res.ok) {
				setError('Demo login failed.')
				return
			}
			router.push('/dashboard')
		} catch {
			setError('Network error.')
		} finally {
			setDemoLoading(false)
		}
	}

	return (
		<div
			style={{
				height: '100vh',
				width: '100vw',
				overflow: 'hidden',
				display: 'flex',
				flexDirection: 'column',
				background: '#0c0e12',
				backgroundImage:
					'radial-gradient(ellipse 900px 700px at 50% 46%, rgba(25,30,45,0.55) 0%, transparent 70%)',
				fontFamily: 'var(--font-sans)',
			}}
		>
			{/* ── Navbar (same component as dashboard) ── */}
			<TopNav
				background="#0c0e12"
				borderless
				right={authState === 'authenticated' ? <NavUserIcons /> : undefined}
			/>

			{/* ── Hero ── */}
			<main
				style={{
					flex: 1,
					display: 'flex',
					flexDirection: 'column',
					alignItems: 'center',
					justifyContent: 'center',
					gap: 0,
				}}
			>
				{/* Eyebrow */}
				<p
					style={{
						margin: '0 0 24px',
						fontFamily: 'var(--font-mono)',
						fontSize: 10,
						letterSpacing: '0.22em',
						color: 'var(--color-text-muted)',
						textTransform: 'uppercase',
					}}
				>
					Geospatial intelligence · v1
				</p>

				{/* Wordmark */}
				<h1
					style={{
						margin: 0,
						fontSize: 'clamp(42px, 5vw, 60px)',
						fontWeight: 300,
						letterSpacing: '0.18em',
						color: 'var(--color-text-primary)',
						textTransform: 'uppercase',
					}}
				>
					Sentinel
				</h1>

				{/* Description */}
				<p
					style={{
						margin: '16px 0 40px',
						fontSize: 13,
						lineHeight: 1.6,
						color: 'var(--color-text-muted)',
						maxWidth: 340,
						textAlign: 'center',
					}}
				>
					Monitor, detect, and act on geospatial events in real time.
				</p>

				{/* CTAs */}
				{authState === 'checking' ? null : authState === 'authenticated' ? (
					<a
						href="/dashboard"
						style={{
							padding: '9px 24px',
							border: '1px solid var(--color-border)',
							borderRadius: 4,
							fontSize: 12,
							color: 'var(--color-text-secondary)',
							textDecoration: 'none',
							letterSpacing: '0.04em',
						}}
					>
						Open Dashboard
					</a>
				) : (
					<div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: 14 }}>
						<div style={{ display: 'flex', gap: 10, alignItems: 'center' }}>
							<button
								onClick={handleDemo}
								disabled={demoLoading}
								style={{
									padding: '9px 22px',
									border: '1px solid var(--color-border)',
									borderRadius: 4,
									background: 'transparent',
									fontSize: 12,
									color: 'var(--color-text-secondary)',
									cursor: demoLoading ? 'not-allowed' : 'pointer',
									opacity: demoLoading ? 0.5 : 1,
									letterSpacing: '0.04em',
									fontFamily: 'var(--font-sans)',
								}}
							>
								{demoLoading ? 'Starting…' : 'Try Demo'}
							</button>

							{GOOGLE_CLIENT_ID && (
								<GoogleLogin
									onSuccess={handleGoogleSuccess}
									onError={() => setError('Google sign-in failed.')}
									theme="filled_black"
									size="medium"
									text="signin_with"
									shape="rectangular"
								/>
							)}
						</div>

						<p
							style={{
								margin: 0,
								fontSize: 10,
								color: 'var(--color-text-muted)',
								fontFamily: 'var(--font-mono)',
								letterSpacing: '0.06em',
							}}
						>
							Demo · 3 min · no account required
						</p>

						{error && (
							<p
								role="alert"
								style={{
									margin: 0,
									fontSize: 11,
									color: 'var(--color-status-critical)',
									fontFamily: 'var(--font-mono)',
								}}
							>
								{error}
							</p>
						)}
					</div>
				)}
			</main>

			{/* ── Footer (same as dashboard) ── */}
			<Footer />
		</div>
	)
}
