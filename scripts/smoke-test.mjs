import assert from 'node:assert/strict';

const esm = await import('../dist/index.js');
assert.equal(typeof esm.SqlShardProvider, 'function');
assert.equal(typeof esm.defineCollection, 'function');
assert.equal(typeof esm.createSqlAdapter, 'function');
assert.equal(typeof esm.createDataConnectAdapter, 'function');
assert.equal(typeof esm.createShardedActions, 'function');

const cjs = await import('../dist/index.cjs');
assert.equal(typeof cjs.SqlShardProvider, 'function');
assert.equal(typeof cjs.defineCollection, 'function');

console.log('Smoke test passed: dist exports are loadable in ESM and CJS.');
