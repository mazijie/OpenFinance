const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync('backend/static/app.js', 'utf8');
const start = source.indexOf('function renderQuickNews(');
const end = source.indexOf('\nfunction renderNewsItems(', start);
const quickSource = source.slice(start, end);

function setup(fetch) {
  const nodes = {'#quickNews': {}, '#quickReports': {}, '#quickReportsSymbol': {}};
  const symbolInput = {value: '000001.SZ'};
  const context = vm.createContext({
    document: {querySelector: selector => nodes[selector]}, symbolInput,
    state: {securities: [{symbol: '000001.SZ', name: '平安银行'}, {symbol: '600519.SH', name: '贵州茅台'}]},
    escapeHtml: value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])),
    formatDateTime: value => value || '未知时间', URLSearchParams, fetch,
  });
  vm.runInContext(quickSource, context);
  return {nodes, symbolInput, context};
}

test('quick news shows latest three safely and suppresses unsafe URLs', () => {
  const {nodes, context} = setup();
  context.renderQuickNews([{title:'<标题>', url:'javascript:alert(1)'}, {title:'新闻二', url:'https://example.com'}, {title:'新闻三'}, {title:'新闻四'}]);
  assert.match(nodes['#quickNews'].innerHTML, /&lt;标题&gt;/);
  assert.doesNotMatch(nodes['#quickNews'].innerHTML, /javascript:|新闻四/);
  assert.match(nodes['#quickNews'].innerHTML, /https:\/\/example.com/);
});

test('quick reports use six-digit code, ignore stale results, and clear on deselection', async () => {
  const pending = [];
  const {nodes, symbolInput, context} = setup(url => new Promise(resolve => pending.push({url, resolve})));
  const first = context.loadQuickReports();
  assert.match(pending[0].url, /stock_code=000001/);
  symbolInput.value = '600519.SH';
  const second = context.loadQuickReports();
  assert.match(pending[1].url, /stock_code=600519/);
  pending[1].resolve({ok:true, json:async () => ({items:[{report_id:'new', title:'新年报', report_year:2025}]})});
  await second;
  pending[0].resolve({ok:true, json:async () => ({items:[{report_id:'old', title:'旧年报', report_year:2024}]})});
  await first;
  assert.match(nodes['#quickReports'].innerHTML, /新年报/);
  assert.doesNotMatch(nodes['#quickReports'].innerHTML, /旧年报/);
  assert.equal(nodes['#quickReportsSymbol'].textContent, '贵州茅台 · 600519.SH');
  symbolInput.value = '';
  await context.loadQuickReports();
  assert.equal(nodes['#quickReports'].textContent, '选择证券后查看年报。');
});
