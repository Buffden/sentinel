import type { NextFunction, Request, RequestHandler, Response } from 'express';

// Express 4 does not forward a rejected promise from an async route
// handler to error-handling middleware on its own -- an uncaught rejection
// leaves the request hanging with no response, rather than reaching
// index.ts's error-handling middleware. Every async handler must be
// wrapped with this so its rejection is explicitly forwarded via next(err).
export function asyncHandler(
	handler: (req: Request, res: Response, next: NextFunction) => Promise<void>,
): RequestHandler {
	return (req, res, next) => {
		handler(req, res, next).catch(next);
	};
}
