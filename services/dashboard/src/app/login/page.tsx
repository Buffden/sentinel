'use client'

import { useEffect } from 'react'
import { useRouter } from 'next/navigation'

// /login is no longer a standalone page. Auth is handled on the landing page (/).
export default function LoginRedirect() {
	const router = useRouter()
	useEffect(() => {
		router.replace('/')
	}, [router])
	return null
}
