const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

function setup() {
  const handlers = {};
  const symbol = {value: '', selectedOptions: [{dataset: {exchange: 'SZ'}}], addEventListener: (name, fn) => {handlers[name] = fn;}};
  const status = {textContent: ''};
  const nodes = ['return_5d', 'ma_5d', 'volume_ratio_5d'].map(field => ({dataset: {gold: field}, textContent: '', classList: {add() {}, remove() {}}}));
  const dummy = {addEventListener() {}};
  const pending = [];
  const context = {document: {querySelector: s => ({'#symbolInput':symbol, '#goldMetricsStatus':status, '#goldScreen':dummy, '#goldScreenForm':dummy, '#goldScreenResult':dummy, '#goldScreenDate':dummy, '#goldScreenNote':dummy}[s]), querySelectorAll: selector => selector === '[data-gold]' ? nodes : []},
    fetch: url => url.includes('/ranking?') ? Promise.resolve({ok: true, json: async () => ({items: []})}) : new Promise(resolve => pending.push({url, resolve})), URLSearchParams, Number, Set};
  vm.runInNewContext(fs.readFileSync('backend/static/market-gold.js', 'utf8'), context);
  return {symbol, status, nodes, pending, handlers};
}

test('selected symbol loads metrics and ignores results from an older selection', async () => {
  const {symbol, status, nodes, pending, handlers} = setup();
  symbol.value = '000001';
  const first = handlers.change();
  assert.match(pending[0].url, /symbol=000001.SZ/);
  symbol.value = '600519'; symbol.selectedOptions[0].dataset.exchange = 'SH';
  const second = handlers.change();
  pending[1].resolve({ok: true, json: async () => ({trading_date: '2026-11-05', items: [{symbol: '600519.SH', return_5d: .05, ma_5d: 12.34, volume_ratio_5d: 1.2, data_status: 'partial'}]})});
  await second;
  pending[0].resolve({ok: true, json: async () => ({items: [{return_5d: -.2}]})});
  await first;
  assert.equal(status.textContent, '2026-11-05 · 部分指标缺失');
  assert.deepEqual(nodes.map(n => n.textContent), ['+5.00%', '12.34', '1.2 倍']);
  symbol.value = '';
  await handlers['security-update']();
  assert.equal(status.textContent, '选择证券后加载');
  assert.deepEqual(nodes.map(n => n.textContent), ['—', '—', '—']);
});

test('missing and unavailable metrics have distinct states', async () => {
  const {symbol, status, nodes, pending, handlers} = setup();
  symbol.value = '000001.SZ';
  const empty = handlers.change();
  pending[0].resolve({ok: true, json: async () => ({items: []})});
  await empty;
  assert.equal(status.textContent, '暂无该证券指标');
  const error = handlers.change();
  pending[1].resolve({ok: false, status: 500});
  await error;
  assert.equal(status.textContent, '行情指标暂不可用');
  assert.equal(nodes[0].textContent, '—');
});
