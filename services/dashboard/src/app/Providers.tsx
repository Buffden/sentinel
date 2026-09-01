'use client'

import { GoogleOAuthProvider } from '@react-oauth/google'

const GOOGLE_CLIENT_ID = process.env['NEXT_PUBLIC_GOOGLE_CLIENT_ID'] ?? ''

export default function Providers({ children }: { children: React.ReactNode }) {
	// GoogleOAuthProvider is always mounted; the Google button conditionally renders
	// only when NEXT_PUBLIC_GOOGLE_CLIENT_ID is set.
	return <GoogleOAuthProvider clientId={GOOGLE_CLIENT_ID}>{children}</GoogleOAuthProvider>
}
