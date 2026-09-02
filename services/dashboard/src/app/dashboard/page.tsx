'use client'

import { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import TopNav from '@/widgets/top-nav/TopNav'
import Footer from '@/shell/Footer'
import Workspace from '@/workspace/Workspace'
import DemoExpiredBanner from '@/shared/ui/DemoExpiredBanner'
import NavUserIcons from '@/shared/ui/NavUserIcons'

type AuthState = 'checking' | 'ok' | 'redirecting'


export default function DashboardPage() {
	const router = useRouter()
	const [authState, setAuthState] = useState<AuthState>('checking')
	// Triggered by the live-feed hook (CP7f) when WS closes with code 4401.
	const [demoExpired, setDemoExpired] = useState(false)

	useEffect(() => {
		fetch('/api/healthz-auth')
			.then((res) => {
				if (res.status === 401) {
					setAuthState('redirecting')
					router.replace('/')
				} else {
					setAuthState('ok')
				}
			})
			.catch(() => {
				setAuthState('redirecting')
				router.replace('/')
			})
	}, [router])

	if (authState === 'checking' || authState === 'redirecting') {
		return (
			<div
				style={{
					display: 'flex',
					height: '100vh',
					alignItems: 'center',
					justifyContent: 'center',
					background: 'var(--color-bg-app)',
					fontFamily: 'var(--font-mono)',
					fontSize: 'var(--font-size-xs)',
					color: 'var(--color-text-muted)',
					letterSpacing: '0.08em',
				}}
			>
				{authState === 'checking' ? 'Checking session…' : 'Redirecting…'}
			</div>
		)
	}

	return (
		<div
			style={{
				display: 'flex',
				flexDirection: 'column',
				height: '100vh',
				width: '100vw',
				overflow: 'hidden',
			}}
		>
			{demoExpired && <DemoExpiredBanner onDismiss={() => setDemoExpired(false)} />}
			<TopNav right={<NavUserIcons />} />
			<Workspace onDemoExpired={() => setDemoExpired(true)} />
			<Footer />
		</div>
	)
}
