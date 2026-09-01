// Thin fetch wrapper for API calls made from client components.
// Redirects to / on 401 so components never have to handle unauthenticated state themselves.
// All paths are relative to the Next.js origin — proxied to the API via next.config rewrites.

export async function fetchApi(path: string, init?: RequestInit): Promise<Response> {
	const res = await fetch(path, init)
	if (res.status === 401) {
		window.location.replace('/')
		// Throw so callers can stop processing; redirect is async from browser's perspective.
		throw new Error('Unauthenticated')
	}
	return res
}
