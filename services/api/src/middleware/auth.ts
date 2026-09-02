import type { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { config } from '../config.js';

export interface SentinelJwtPayload {
	user_id: string;
	email: string;
	name?: string;
	role: 'operator' | 'demo';
	// Standard JWT claim: seconds since epoch. Present on all tokens we issue.
	exp?: number;
}

// Attaches user_id and email to the request after verifying the sentinel_jwt cookie.
// Returns 401 for missing, tampered, or expired tokens.
export function requireAuth(req: Request, res: Response, next: NextFunction): void {
	const token = (req.cookies as Record<string, string | undefined>)['sentinel_jwt'];
	if (!token) {
		res.status(401).json({ error: 'Authentication required' });
		return;
	}
	try {
		const payload = jwt.verify(token, config.JWT_SECRET) as unknown as SentinelJwtPayload;
		res.locals['userId'] = payload.user_id;
		res.locals['userEmail'] = payload.email;
		next();
	} catch {
		res.status(401).json({ error: 'Invalid or expired token' });
	}
}
