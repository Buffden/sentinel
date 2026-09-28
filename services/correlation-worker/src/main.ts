// Correlation Worker entry point (`npm run -s worker`). Kept separate from
// worker.ts so integration tests can import worker.ts without the process
// handlers.

// Must stay the first import (see processHandlers.ts).
import './processHandlers.js';
import { start } from './worker.js';

start();
