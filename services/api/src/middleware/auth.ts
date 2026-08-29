import type { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';

const JWT_SECRET = process.env['JWT_SECRET'];
if (!JWT_SECRET) throw new Error('JWT_SECRET env var is required');
const secret: string = JWT_SECRET;

export interface SentinelJwtPayload {
  user_id: string;
  email: string;
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
    const payload = jwt.verify(token, secret) as unknown as SentinelJwtPayload;
    res.locals['userId'] = payload.user_id;
    res.locals['userEmail'] = payload.email;
    next();
  } catch {
    res.status(401).json({ error: 'Invalid or expired token' });
  }
}
