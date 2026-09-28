// Alert Evaluator entry point (`npm run -s evaluator`). Kept separate from
// evaluator.ts so integration tests can import evaluator.ts without the
// process handlers.

// Must stay the first import (see processHandlers.ts).
import './processHandlers.js';
import { start } from './evaluator.js';

start();
