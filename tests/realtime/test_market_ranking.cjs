const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

test('ranking loads by period and ignores a stale response after tab switches', async () => {
  let click;
  const tabs = ['day', 'five_days', 'month', 'year'].map(period => ({
    dataset: {rankingPeriod: period}, classList: {toggle() {}}, setAttribute() {},
  }));
  const sides = ['positive', 'negative'].map(side => ({dataset: {rankingSide: side}, classList: {toggle() {}}, setAttribute() {}}));
  const screen = {addEventListener: (_, fn) => { click = fn; }, contains: el => tabs.includes(el) || sides.includes(el)};
  const result = {textContent: '', innerHTML: ''};
  const date = {textContent: ''};
  const note = {textContent: ''};
  const symbolEvents = {};
  const symbol = {value: '', options: [], addEventListener: (name, fn) => { symbolEvents[name] = fn; }};
  const pending = [];
  const context = {
    document: {querySelector: selector => ({'#goldScreen': screen, '#goldScreenDate': date, '#goldScreenNote': note, '#goldScreenResult': result, '#symbolInput': symbol, '#goldMetricsStatus': {}}[selector]),
      querySelectorAll: selector => selector === '[data-ranking-period]' ? tabs : selector === '[data-ranking-side]' ? sides : []},
    fetch: url => new Promise(resolve => pending.push({url, resolve})), URLSearchParams, Number, Set,
  };
  vm.runInNewContext(fs.readFileSync('backend/static/market-gold.js', 'utf8'), context);
  assert.match(pending[0].url, /period=day/);
  click({target: {closest: () => tabs[3]}});
  assert.match(pending[1].url, /period=year/);
  pending[1].resolve({ok: true, json: async () => ({trading_date: '2026-11-05', side: 'positive', return_field: 'return_1y', items: [{symbol: '600519.SH', close: 12, return_1y: .125}]})});
  await new Promise(resolve => setImmediate(resolve));
  assert.match(result.innerHTML, /600519.SH/);
  assert.match(result.innerHTML, /\+12\.50%/);
  assert.match(result.innerHTML, /证券名称/);
  symbol.options = [{value: '600519', dataset: {exchange: 'SH', name: '贵州茅台'}}];
  symbolEvents['security-update']();
  assert.match(result.innerHTML, /贵州茅台/);
  assert.match(note.textContent, /^近一年 · 收益从高到低 · 不复权/);
  pending[0].resolve({ok: true, json: async () => ({trading_date: '2026-11-04', return_field: 'return_1d', items: [{symbol: 'STALE', close: 1, return_1d: .1}]})});
  await new Promise(resolve => setImmediate(resolve));
  assert.doesNotMatch(result.innerHTML, /STALE/);
  assert.equal(date.textContent, '截至 2026-11-05');
  click({target: {closest: selector => selector === '[data-ranking-side]' ? sides[1] : null}});
  assert.match(pending[2].url, /period=year/);
  assert.match(pending[2].url, /side=negative/);
  pending[2].resolve({ok: true, json: async () => ({trading_date: '2026-11-05', side: 'negative', return_field: 'return_1y', items: [{symbol: '600519.SH', close: 12, return_1y: -.125}]})});
  await new Promise(resolve => setImmediate(resolve));
  assert.match(result.innerHTML, /-12\.50%/);
  assert.match(note.textContent, /收益从低到高/);
});
