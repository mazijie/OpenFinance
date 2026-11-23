(() => {
  const screen = document.querySelector('#goldScreen');
  const rankingDate = document.querySelector('#goldScreenDate');
  const rankingNote = document.querySelector('#goldScreenNote');
  const rankingTabs = [...document.querySelectorAll('[data-ranking-period]')];
  const rankingSides = [...document.querySelectorAll('[data-ranking-side]')];
  let rankingPeriod = 'day';
  let rankingSide = 'positive';
  const result = document.querySelector('#goldScreenResult');
  const escape = (value) => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const value = (v, percent = false) => v == null ? '—' : percent ? `${(Number(v) * 100).toFixed(2)}%` : Number(v).toLocaleString('zh-CN', {maximumFractionDigits: 2});
  async function get(url) {
    const response = await fetch(url);
    if (!response.ok) throw new Error(`查询失败 (${response.status})`);
    return response.json();
  }
  let rankingRequestId = 0;
  let rankingData = null;
  function renderRanking(data) {
    const names = new Map();
    for (const option of symbolInput.options || []) {
      const code = String(option.value || '').toUpperCase();
      const exchange = String(option.dataset.exchange || '').toUpperCase();
      const fullCode = /^\d{6}$/.test(code) && exchange ? `${code}.${exchange}` : code;
      if (fullCode && option.dataset.name) names.set(fullCode, option.dataset.name);
    }
    const field = data.return_field;
    result.innerHTML = data.items.length ? `<table><thead><tr><th>排名</th><th>证券代码</th><th>证券名称</th><th>收盘价</th><th>区间收益</th></tr></thead><tbody>${data.items.map((row, index) => {
      const number = Number(row[field]);
      const color = number > 0 ? 'up-text' : number < 0 ? 'down-text' : '';
      const change = row[field] == null || !Number.isFinite(number) ? '—' : `${number > 0 ? '+' : ''}${value(number, true)}`;
      const name = names.get(String(row.symbol || '').toUpperCase()) || '—';
      return `<tr><td>${index + 1}</td><td>${escape(row.symbol)}</td><td>${escape(name)}</td><td>${value(row.close)}</td><td class="${color}">${change}</td></tr>`;
    }).join('')}</tbody></table>` : `<p class="ranking-empty">该周期暂无${rankingSide === 'positive' ? '正' : '负'}收益数据。</p>`;
  }
  async function loadScreen(period) {
    const id = ++rankingRequestId;
    rankingData = null;
    result.textContent = '正在加载排行…';
    rankingDate.textContent = '正在加载…';
    const order = rankingSide === 'positive' ? '收益从高到低' : '收益从低到高';
    rankingNote.textContent = period === 'month' ? `最近 20 条行情记录 · ${order} · 不复权` : period === 'year' ? `近一年 · ${order} · 不复权，基准缺失超过 14 天不纳入` : `${order} · 不复权`;
    try {
      const data = await get(`/api/market/gold/ranking?${new URLSearchParams({period, side: rankingSide, limit: '20'})}`);
      if (id !== rankingRequestId) return;
      if (data.side !== rankingSide) throw new Error('排行方向不匹配');
      rankingDate.textContent = data.trading_date ? `截至 ${data.trading_date}` : '暂无交易日';
      rankingData = data;
      renderRanking(data);
    } catch (error) {
      if (id !== rankingRequestId) return;
      rankingDate.textContent = '加载失败';
      result.textContent = '行情排行暂不可用，请稍后重试。';
    }
  }
  const symbolInput = document.querySelector('#symbolInput');
  const metricsStatus = document.querySelector('#goldMetricsStatus');
  const metricNodes = [...document.querySelectorAll('[data-gold]')];
  const percentFields = new Set(['return_1d', 'return_5d', 'return_20d', 'price_ma_5d_deviation', 'price_ma_20d_deviation', 'volatility_20d_annualized']);
  const signedFields = new Set(['return_1d', 'return_5d', 'return_20d', 'price_ma_5d_deviation', 'price_ma_20d_deviation']);
  let metricsRequestId = 0;

  function resetMetrics() {
    for (const node of metricNodes) { node.textContent = '—'; node.classList.remove('market-up', 'market-down'); }
  }

  async function loadMetrics() {
    const id = ++metricsRequestId;
    const selected = symbolInput.value;
    resetMetrics();
    if (!selected) { metricsStatus.textContent = '选择证券后加载'; return; }
    const exchange = symbolInput.selectedOptions[0]?.dataset.exchange?.toUpperCase();
    const symbol = /^\d{6}$/.test(selected) && ['SH', 'SZ', 'BJ'].includes(exchange) ? `${selected}.${exchange}` : selected;
    metricsStatus.textContent = '正在加载…';
    try {
      const data = await get(`/api/market/gold/screen?${new URLSearchParams({symbol, limit: '1'})}`);
      if (id !== metricsRequestId) return;
      const row = data.items?.[0];
      if (!row) { metricsStatus.textContent = '暂无该证券指标'; return; }
      if (String(row.symbol || '').toUpperCase() !== symbol.toUpperCase()) throw new Error('行情指标与所选证券不匹配');
      metricsStatus.textContent = `${data.trading_date || row.trading_date || '日期未知'} · ${row.data_status === 'partial' ? '部分指标缺失' : '日线指标'}`;
      for (const node of metricNodes) {
        const field = node.dataset.gold;
        const raw = row[field];
        if (raw == null || raw === '' || !Number.isFinite(Number(raw))) continue;
        const number = Number(raw);
        const signed = signedFields.has(field) && number > 0 ? '+' : '';
        node.textContent = signed + value(number, percentFields.has(field)) + (field === 'volume_ratio_5d' ? ' 倍' : '');
        if (signedFields.has(field) && number !== 0) node.classList.add(number > 0 ? 'market-up' : 'market-down');
      }
    } catch (error) {
      if (id === metricsRequestId) metricsStatus.textContent = '行情指标暂不可用';
    }
  }
  symbolInput.addEventListener('change', loadMetrics);
  symbolInput.addEventListener('security-update', () => {
    loadMetrics();
    if (rankingData) renderRanking(rankingData);
  });
  if (symbolInput.value) loadMetrics();

  screen.addEventListener('click', event => {
    const periodButton = event.target.closest('[data-ranking-period]');
    if (periodButton && screen.contains(periodButton)) {
      rankingPeriod = periodButton.dataset.rankingPeriod;
      for (const tab of rankingTabs) {
        const active = tab === periodButton;
        tab.classList.toggle('active', active);
        tab.setAttribute('aria-selected', String(active));
      }
      loadScreen(rankingPeriod);
      return;
    }
    const sideButton = event.target.closest('[data-ranking-side]');
    if (!sideButton || !screen.contains(sideButton)) return;
    rankingSide = sideButton.dataset.rankingSide;
    for (const button of rankingSides) {
      const active = button === sideButton;
      button.classList.toggle('active', active);
      button.setAttribute('aria-pressed', String(active));
    }
    loadScreen(rankingPeriod);
  });
  const factorForm = document.querySelector('#factorScreenForm');
  if (factorForm) {
    const fields = {
      return_1d: '1 日收益', return_5d: '5 日收益', return_20d: '20 日收益', return_60d: '60 日收益', return_120d: '120 日收益',
      ma_60d: '60 日均价', ma_120d: '120 日均价', volatility_20d_annualized: '20 日波动率',
      volatility_60d_annualized: '60 日波动率', max_drawdown_60d: '60 日最大回撤', amount: '成交额',
      avg_amount_20d: '20 日平均成交额', amount_change_20d: '成交额变化率', turnover: '换手率',
      avg_turnover_20d: '20 日平均换手率', avg_turnover_60d: '60 日平均换手率'
    };
    const percentileBases = ['return_1d', 'return_5d', 'return_20d', 'return_60d', 'return_120d',
      'volatility_20d_annualized', 'volatility_60d_annualized', 'max_drawdown_60d', 'amount', 'avg_amount_20d',
      'amount_change_20d', 'turnover', 'avg_turnover_20d', 'avg_turnover_60d'];
    for (const field of percentileBases) fields[`${field}_percentile`] = `${fields[field]}分位数`;
    const options = Object.entries(fields).map(([key, label]) => `<option value="${key}">${label}</option>`).join('');
    const sort = document.querySelector('#factorSort');
    const conditions = document.querySelector('#factorConditions');
    const output = document.querySelector('#factorScreenResult');
    sort.innerHTML = options;
    function addCondition() {
      if (conditions.children.length >= 20) return;
      const row = document.createElement('div');
      row.className = 'factor-controls factor-condition';
      row.innerHTML = `<label>指标 <select aria-label="条件指标">${options}</select></label><label>下限 <input type="number" step="any" aria-label="下限"></label><label>上限 <input type="number" step="any" aria-label="上限"></label><button type="button" class="secondary-button" aria-label="删除条件">删除</button>`;
      row.querySelector('button').addEventListener('click', () => row.remove());
      conditions.appendChild(row);
    }
    document.querySelector('#factorAdd').addEventListener('click', addCondition);
    addCondition();
    let requestId = 0;
    factorForm.addEventListener('submit', async event => {
      event.preventDefault();
      const filters = [];
      for (const row of conditions.children) {
        const [min, max] = row.querySelectorAll('input');
        if (min.value === '' && max.value === '') continue;
        const filter = {field: row.querySelector('select').value};
        if (min.value !== '') filter.min = Number(min.value);
        if (max.value !== '') filter.max = Number(max.value);
        filters.push(filter);
      }
      const params = new URLSearchParams({sort_by: sort.value, direction: factorForm.elements.direction.value, filters: JSON.stringify(filters), limit: '100'});
      if (factorForm.elements.trading_date.value) params.set('trading_date', factorForm.elements.trading_date.value);
      const id = ++requestId;
      output.textContent = '正在查询…';
      try {
        const data = await get(`/api/market/gold/screen?${params}`);
        if (id !== requestId) return;
        const field = params.get('sort_by');
        output.innerHTML = data.items.length ? `<p>交易日 ${escape(data.trading_date)} · ${data.items.length} 条</p><table><thead><tr><th>证券</th><th>${escape(fields[field])}</th><th>收盘价</th></tr></thead><tbody>${data.items.map(row => `<tr><td>${escape(row.symbol)}</td><td>${value(row[field])}</td><td>${value(row.close)}</td></tr>`).join('')}</tbody></table>` : '<p>没有符合条件的证券。</p>';
      } catch (error) { if (id === requestId) output.textContent = '筛选失败，请检查条件后重试。'; }
    });
  }
  loadScreen('day');
})();
