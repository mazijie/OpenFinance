const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');

const source = fs.readFileSync('backend/static/app.js', 'utf8');
const start = source.indexOf('function aggregateBars(');
const end = source.indexOf('\nfunction chinaDate', start);
const aggregate = vm.runInNewContext(`(${source.slice(start, end)})`);
const liveStart = source.indexOf('function chinaDate(');
const liveEnd = source.indexOf('\nfunction selectedQuoteSymbol', liveStart);
const liveBar = vm.runInNewContext(`${source.slice(liveStart, liveEnd)}; provisionalDailyBar`);
const intradayStart = source.indexOf('function intradayTradingMinute(');
const intradayEnd = source.indexOf('\nfunction clearMarket', intradayStart);
const intraday = vm.runInNewContext(`${source.slice(intradayStart, intradayEnd)}; ({ intradayTradingMinute, intradaySeries, intradayPreviousClose })`);

test('weekly and monthly candles use first open, last close and all daily volumes', () => {
  const bars = [
    { trading_date: '2026-08-03', open: 10, high: 12, low: 9, close: 11, volume: 100 },
    { trading_date: '2026-08-04', open: 11, high: 14, low: 10, close: 13, volume: 200 },
    { trading_date: '2026-08-10', open: 13, high: 15, low: 12, close: 14, volume: 300 },
  ];
  const weeks = aggregate(bars, 'weekly');
  assert.equal(weeks.length, 2);
  assert.equal(weeks[0].open, 10);
  assert.equal(weeks[0].high, 14);
  assert.equal(weeks[0].low, 9);
  assert.equal(weeks[0].close, 13);
  assert.equal(weeks[0].volume, 300);
  assert.equal(weeks[0].period_first_date, '2026-08-03');
  const month = aggregate(bars, 'monthly');
  assert.equal(month.length, 1);
  assert.equal(month[0].close, 14);
  assert.equal(month[0].volume, 600);
});

test('intraday quote makes a provisional day without replacing official bars', () => {
  const now = Date.parse('2026-11-02T06:00:00Z');
  const quote = {symbol: '600519.SH', status: 'fresh', quote_status: 'valid', source_time: '2026-11-02T05:59:00Z', captured_at: '2026-11-02T05:59:05Z', day_open: 10, day_high: 15, day_low: 9, price: 12, volume_lots: 123};
  const historical = [{trading_date: '2026-10-30', open: 9, high: 11, low: 8, close: 10, volume: 1000}];
  const bar = liveBar(quote, historical, '600519.SH', now);
  assert.equal(bar.trading_date, '2026-11-02');
  assert.equal(bar.volume, 12300);
  assert.equal(bar.provisional, true);
  assert.equal(liveBar(quote, [...historical, {...bar, provisional: false}], '600519.SH', now), null);
  assert.equal(liveBar({...quote, status: 'not_updated'}, historical, '600519.SH', now), null);
  assert.equal(liveBar({...quote, day_high: 11}, historical, '600519.SH', now), null);
  assert.equal(liveBar({...quote, source_time: '2026-11-01T06:00:00Z'}, historical, '600519.SH', now), null);
  assert.equal(liveBar({...quote, symbol: '000001.SZ'}, historical, '600519.SH', now), null);
  assert.equal(liveBar({...quote, volume_lots: null}, historical, '600519.SH', now).volume, null);
  assert.equal(liveBar({...quote, status: 'closed'}, historical, '600519.SH', now).close, 12);
  assert.equal(liveBar({...quote, status: 'source_error'}, historical, '600519.SH', now), null);
  assert.equal(liveBar({...quote, day_open: null}, historical, '600519.SH', now), null);
  const week = aggregate([bar, {trading_date: '2026-11-03', open: 12, high: 14, low: 8, close: 11, volume: 500}], 'weekly')[0];
  assert.equal(week.open, 10);
  assert.equal(week.high, 15);
  assert.equal(week.low, 8);
  assert.equal(week.volume, 12800);
  assert.equal(week.provisional, true);
  assert.equal(aggregate([bar, {trading_date: '2026-11-03', open: 12, high: 14, low: 8, close: 11, volume: null}], 'weekly')[0].volume, null);
});

test('current weekly and monthly bars combine provisional day with existing daily bars', () => {
  const now = Date.parse('2026-11-04T06:00:00Z');
  const history = [
    {trading_date: '2026-11-02', open: 10, high: 12, low: 9, close: 11, volume: 1000},
    {trading_date: '2026-11-03', open: 11, high: 13, low: 10, close: 12, volume: 2000},
  ];
  const quote = {symbol: '600519.SH', status: 'fresh', quote_status: 'valid',
    source_time: '2026-11-04T05:59:00Z', captured_at: '2026-11-04T05:59:05Z',
    day_open: 12, day_high: 14, day_low: 8, price: 13, volume_lots: 50};
  const today = liveBar(quote, history, '600519.SH', now);
  for (const period of ['weekly', 'monthly']) {
    const result = aggregate([...history, today], period);
    assert.equal(result.length, 1);
    assert.equal(result[0].open, 10);
    assert.equal(result[0].high, 14);
    assert.equal(result[0].low, 8);
    assert.equal(result[0].close, 13);
    assert.equal(result[0].volume, 8000);
    assert.equal(result[0].provisional, true);
  }
  const official = {...today, close: 12.5, provisional: false};
  assert.equal(liveBar(quote, [...history, official], '600519.SH', now), null);
  assert.equal(aggregate([...history, official], 'weekly')[0].close, 12.5);
});

test('A-share intraday axis compresses lunch while retaining session gaps', () => {
  const minute = intraday.intradayTradingMinute;
  assert.equal(minute('2026-11-02T01:30:00Z'), 0); // 09:30 Shanghai
  assert.equal(minute('2026-11-02T03:30:00Z'), 120); // 11:30 Shanghai
  assert.equal(minute('2026-11-02T05:00:00Z'), 120); // 13:00 Shanghai
  assert.equal(minute('2026-11-02T07:00:00Z'), 240); // 15:00 Shanghai
  assert.equal(minute('2026-11-02T04:00:00Z'), null); // lunch
  assert.equal(minute('2026-11-02T01:00:00Z'), null); // before open
  assert.equal(minute('2026-11-02T07:01:00Z'), null); // after close

  const series = intraday.intradaySeries([
    { time: '2026-11-02T03:29:00Z', price: 10 },
    { time: '2026-11-02T05:00:00Z', price: 11 },
    { time: '2026-11-02T05:02:00Z', price: 12 },
    { time: '2026-11-02T05:03:00Z', price: 13 },
    { time: '2026-11-02T04:00:00Z', price: 99 },
  ]);
  assert.deepEqual(Array.from(series, (point) => [point.value[0], point.value[1]]), [[119, 10], [120, 11], [122, null], [122, 12], [123, 13]]);
  assert.equal(series[1].time, '2026-11-02T05:00:00Z');
});

test('intraday previous close accepts only a positive finite price', () => {
  assert.equal(intraday.intradayPreviousClose(12.34), 12.34);
  assert.equal(intraday.intradayPreviousClose('12.34'), 12.34);
  assert.equal(intraday.intradayPreviousClose(null), null);
  assert.equal(intraday.intradayPreviousClose(0), null);
  assert.equal(intraday.intradayPreviousClose('invalid'), null);
});
