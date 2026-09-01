import type { NextConfig } from 'next'

const nextConfig: NextConfig = {
	async rewrites() {
		const apiUrl = process.env['API_URL'] ?? 'http://localhost:3001'
		return [
			{
				source: '/api/:path*',
				destination: `${apiUrl}/:path*`,
			},
		]
	},
}

export default nextConfig
