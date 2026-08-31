import type { Metadata } from 'next'
import 'maplibre-gl/dist/maplibre-gl.css'
import 'dockview-react/dist/styles/dockview.css'
import './globals.css'

export const metadata: Metadata = {
	title: 'Sentinel',
	description: 'Real-time geospatial monitoring workspace',
}

export default function RootLayout({ children }: { children: React.ReactNode }) {
	return (
		<html lang="en">
			<head>
				<link
					rel="stylesheet"
					href="https://fonts.googleapis.com/icon?family=Material+Icons"
				/>
			</head>
			<body suppressHydrationWarning>{children}</body>
		</html>
	)
}
