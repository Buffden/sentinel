// Position Consumer entry point (`npm run -s consumer`). Kept separate from
// consumer.ts so integration tests can import consumer.ts without the process
// handlers.

// Must stay the first import (see processHandlers.ts).
import './processHandlers.js';
import { start } from './consumer.js';

start();
