(() => {
  const screen = document.querySelector('#goldScreen');
  const form = document.querySelector('#goldScreenForm');
  const result = document.querySelector('#goldScreenResult');
  const escape = (value) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const value = (v, percent = false) => v == null ? '—' : percent ? `${(Number(v) * 100).toFixed(2)}%` : Number(v).toLocaleString('zh-CN', {maximumFractionDigits: 2});
  async function get(url) {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`查询失败 (${response.status})`);
    return response.json();
  }
  async function loadScreen() {
    result.textContent = '正在查询…';
    const params = new URLSearchParams(new FormData(form));
    for (const name of ['trading_date', 'min_value', 'max_value']) if (!params.get(name)) params.delete(name);
    try {
      const data = await get(`/api/market/gold/screen?${params}`);
      const field = params.get('sort_by');
      result.innerHTML = `<p>${escape(data.trading_date || '暂无交易日')} · ${escape(data.method)}</p>` +
        (data.items.length ? `<div style="overflow-x:auto"><table><thead><tr><th>证券</th><th>收盘价</th><th>排序指标</th><th>成交额</th><th>指标数据</th></tr></thead><tbody>${data.items.map(row => `<tr><td>${escape(row.symbol)}</td><td>${value(row.close)}</td><td>${value(row[field], field.startsWith('return_') || field.includes('_deviation') || field.startsWith('volatility_'))}${field === 'volume_ratio_5d' && row[field] != null ? ' 倍' : ''}</td><td>${value(row.amount)}</td><td>${escape(({complete:'完整',partial:'部分缺失'})[row.data_status] || row.data_status || '未知')}</td></tr>`).join('')}</tbody></table></div>` : '<p>没有符合条件的数据。</p>');
    } catch (error) { result.textContent = `行情横截面暂不可用：${error.message}`; }
  }
  form.addEventListener('submit', event => { event.preventDefault(); loadScreen(); });
  screen.addEventListener('toggle', () => { if (screen.open) loadScreen(); });
})();
