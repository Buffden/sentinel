'use client'

const GITHUB_URL = 'https://github.com/buffden'
const PORTFOLIO_URL = 'https://buffden.com/#/'
const LINKEDIN_URL = 'https://www.linkedin.com/in/harshwardhanpatil23'

function IconLink({
	href,
	label,
	children,
}: {
	href: string
	label: string
	children: React.ReactNode
}) {
	return (
		<a
			href={href}
			target="_blank"
			rel="noopener noreferrer"
			aria-label={label}
			style={{
				color: 'var(--color-text-muted)',
				display: 'flex',
				alignItems: 'center',
				transition: 'color 0.15s',
				textDecoration: 'none',
			}}
			onMouseEnter={(e) => {
				;(e.currentTarget as HTMLAnchorElement).style.color = 'var(--color-text-primary)'
			}}
			onMouseLeave={(e) => {
				;(e.currentTarget as HTMLAnchorElement).style.color = 'var(--color-text-muted)'
			}}
		>
			{children}
		</a>
	)
}

export default function Footer() {
	return (
		<footer
			style={{
				height: 50,
				background: 'var(--color-bg-panel)',
				borderTop: '1px solid var(--color-border)',
				display: 'flex',
				alignItems: 'center',
				padding: '0 var(--space-3)',
				flexShrink: 0,
			}}
		>
			<span
				style={{
					fontFamily: 'var(--font-mono)',
					fontSize: 'var(--font-size-xs)',
					fontWeight: 700,
					letterSpacing: '0.1em',
					textTransform: 'uppercase',
					color: 'var(--color-text-secondary)',
				}}
			>
				Sentinel
			</span>

			<div
				style={{
					flex: 1,
					display: 'flex',
					alignItems: 'center',
					justifyContent: 'center',
					gap: 'var(--space-4)',
				}}
			>
				<IconLink href={GITHUB_URL} label="GitHub">
					<span>GitHub</span>
				</IconLink>
				<IconLink href={LINKEDIN_URL} label="LinkedIn">
					<span>LinkedIn</span>
				</IconLink>
				<IconLink href={PORTFOLIO_URL} label="Portfolio">
					<span>Portfolio</span>
				</IconLink>
			</div>

			<span
				style={{
					fontFamily: 'var(--font-mono)',
					fontSize: 'var(--font-size-xs)',
					color: 'var(--color-text-muted)',
					whiteSpace: 'nowrap',
				}}
			>
				© {new Date().getFullYear()} Harshwardhan Patil
			</span>
		</footer>
	)
}
