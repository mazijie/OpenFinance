const runs = document.querySelector('#runs');
const message = document.querySelector('#message');
const pause = document.querySelector('#pause');
const buttons = [...document.querySelectorAll('button[type="submit"],#batch')];
const labels = { queued:'等待中', running:'执行中', completed:'已结束', blocked:'上游拦截', interrupted:'已中断', failed:'失败', complete:'完成', partial:'部分完成', requesting:'请求中' };
function el(tag, text, className) { const node = document.createElement(tag); node.textContent = text; if (className) node.className = className; return node; }
function time(value) { return value ? new Date(value).toLocaleString('zh-CN', {timeZone:'Asia/Shanghai',hour12:false}) : '—'; }
async function refresh() {
  try {
    const response = await fetch('/api/operations/intraday/runs');
    if (!response.ok) throw new Error('读取任务失败');
    const data = await response.json();
    pause.textContent = data.pause ? `腾讯接口暂停至 ${time(data.pause.paused_until)}：${data.pause.reason}` : '';
    runs.replaceChildren();
    if (!data.runs.length) runs.append(el('p', '暂无任务'));
    for (const run of data.runs) {
      const card = el('article', '', 'run'); const header = el('div', '', 'run-head');
      header.append(el('span', `任务 #${run.id}`), el('span', run.scope === 'batch' ? '全市场一批' : '指定证券'), el('span', labels[run.status] || run.status), el('small', time(run.requested_at)));
      card.append(header);
      if (run.error) card.append(el('p', run.error, 'error'));
      const items = el('div', '', 'items');
      for (const item of run.items) items.append(el('div', `${item.symbol} · ${labels[item.status] || item.status} · 新增 ${item.points_added} 点${item.error ? ` · ${item.error}` : ''}`));
      card.append(items); runs.append(card);
    }
  } catch (error) { runs.textContent = error.message || '读取任务失败'; }
}
async function create(scope, symbol) {
  buttons.forEach(button => button.disabled = true); message.textContent = '正在提交任务…';
  try {
    const response = await fetch('/api/operations/intraday/runs', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({scope, symbol})});
    const data = await response.json();
    if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : '提交失败');
    message.textContent = `任务 #${data.id} 已提交，包含 ${data.symbols.length} 只证券。`;
    await refresh();
  } catch (error) { message.textContent = error.message || '提交失败'; }
  finally { buttons.forEach(button => button.disabled = false); }
}
document.querySelector('#single').addEventListener('submit', event => { event.preventDefault(); create('symbol', document.querySelector('#symbol').value.trim()); });
document.querySelector('#batch').addEventListener('click', () => create('batch', null));
document.querySelector('#refresh').addEventListener('click', refresh);
refresh(); setInterval(refresh, 10000);
