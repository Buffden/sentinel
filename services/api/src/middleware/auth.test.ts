import { describe, expect, it, vi } from 'vitest';
import type { Request, Response } from 'express';
import jwt from 'jsonwebtoken';
import { config } from '../config.js';
import { requireAuth, type SentinelJwtPayload } from './auth.js';

function fakeRequest(cookieToken: string | undefined): Request {
	return { cookies: { sentinel_jwt: cookieToken } } as unknown as Request;
}

function fakeResponse(): Response {
	const res = {
		locals: {},
		status: vi.fn().mockReturnThis(),
		json: vi.fn().mockReturnThis(),
	};
	return res as unknown as Response;
}

function signToken(payload: Omit<SentinelJwtPayload, 'exp'>, options?: jwt.SignOptions): string {
	return jwt.sign(payload, config.JWT_SECRET, { expiresIn: '1h', ...options });
}

describe('requireAuth', () => {
	it('rejects a request with no sentinel_jwt cookie', () => {
		const req = fakeRequest(undefined);
		const res = fakeResponse();
		const next = vi.fn();

		requireAuth(req, res, next);

		expect(res.status).toHaveBeenCalledWith(401);
		expect(res.json).toHaveBeenCalledWith({ error: 'Authentication required' });
		expect(next).not.toHaveBeenCalled();
	});

	it('rejects a token signed with the wrong secret', () => {
		const tampered = jwt.sign(
			{ user_id: 'u1', email: 'u1@example.com', role: 'operator' },
			'wrong-secret',
			{ expiresIn: '1h' },
		);
		const req = fakeRequest(tampered);
		const res = fakeResponse();
		const next = vi.fn();

		requireAuth(req, res, next);

		expect(res.status).toHaveBeenCalledWith(401);
		expect(res.json).toHaveBeenCalledWith({ error: 'Invalid or expired token' });
		expect(next).not.toHaveBeenCalled();
	});

	it('rejects an expired token', () => {
		const expired = signToken(
			{ user_id: 'u1', email: 'u1@example.com', role: 'operator' },
			{ expiresIn: -1 },
		);
		const req = fakeRequest(expired);
		const res = fakeResponse();
		const next = vi.fn();

		requireAuth(req, res, next);

		expect(res.status).toHaveBeenCalledWith(401);
		expect(next).not.toHaveBeenCalled();
	});

	it('rejects a malformed cookie value that is not a JWT at all', () => {
		const req = fakeRequest('not-a-jwt');
		const res = fakeResponse();
		const next = vi.fn();

		requireAuth(req, res, next);

		expect(res.status).toHaveBeenCalledWith(401);
		expect(next).not.toHaveBeenCalled();
	});

	it('accepts a validly signed, unexpired token and attaches identity to res.locals', () => {
		const token = signToken({ user_id: 'u1', email: 'u1@example.com', role: 'operator' });
		const req = fakeRequest(token);
		const res = fakeResponse();
		const next = vi.fn();

		requireAuth(req, res, next);

		expect(next).toHaveBeenCalledTimes(1);
		expect(res.locals['userId']).toBe('u1');
		expect(res.locals['userEmail']).toBe('u1@example.com');
		expect(res.status).not.toHaveBeenCalled();
	});

	it('accepts the demo role the same as operator — role gating happens elsewhere', () => {
		const token = signToken({ user_id: 'demo-1', email: 'demo@example.com', role: 'demo' });
		const req = fakeRequest(token);
		const res = fakeResponse();
		const next = vi.fn();

		requireAuth(req, res, next);

		expect(next).toHaveBeenCalledTimes(1);
	});
});
