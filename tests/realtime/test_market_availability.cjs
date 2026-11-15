const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function setup(fetch) {
  const source = fs.readFileSync('backend/static/app.js', 'utf8');
  const start = source.indexOf('async function checkHistoryAvailability(');
  const end = source.indexOf('\nasync function reconcileFormalBar()', start);
  const context = {symbol: '600519.SH', before: '2025-01-01', snapshot_id: '123',
    table_uuid: 'uuid', query_time: '2026-01-01T00:00:00Z', initialEmpty: false};
  const state = {marketRequestId: 1, marketAbortController: new AbortController(), historyChecking: false,
    historyAvailability: context, historyHasMore: false, historyCheckFailed: false};
  const env = {state, fetch, URLSearchParams, AbortController, setTimeout, clearTimeout,
    MARKET_ADJUSTMENT: 'none', historyStatus: {}, loadOlderButton: {}, symbolInput: {value: '600519.SH'},
    loadHistoryPage: async () => { throw Error('unexpected page load'); }};
  vm.createContext(env);
  vm.runInContext(source.slice(start, end), env);
  return {env, context, state};
}

test('availability updates pagination without reloading the chart', async () => {
  const {env, context, state} = setup(async () => ({ok: true, json: async () => ({has_more: true})}));
  await env.checkHistoryAvailability(1, context);
  assert.equal(state.historyHasMore, true);
  assert.equal(env.loadOlderButton.hidden, false);
  assert.equal(state.historyAvailability, null);
});

test('a failed availability check is retryable and stale results are ignored', async () => {
  const {env, context, state} = setup(async () => { throw new Error('unavailable'); });
  await env.checkHistoryAvailability(1, context);
  assert.equal(state.historyCheckFailed, true);
  assert.equal(state.historyAvailability, context);
  assert.equal(env.loadOlderButton.hidden, false);
  env.fetch = async () => { state.marketRequestId++; return {ok: true, json: async () => ({has_more: true})}; };
  await env.checkHistoryAvailability(1, context);
  assert.equal(state.historyHasMore, false);
});
