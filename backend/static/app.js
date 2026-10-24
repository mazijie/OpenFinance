const chatForm = document.querySelector("#chatForm");
const questionInput = document.querySelector("#questionInput");
const sendButton = document.querySelector("#sendButton");
const messages = document.querySelector("#messages");
const chatStatus = document.querySelector("#chatStatus");
const systemStatus = document.querySelector("#systemStatus");

const marketForm = document.querySelector("#marketForm");
const symbolInput = document.querySelector("#symbolInput");
const startDateInput = document.querySelector("#startDateInput");
const endDateInput = document.querySelector("#endDateInput");
const adjustmentInput = document.querySelector("#adjustmentInput");
const asOfInput = document.querySelector("#asOfInput");
const marketMessage = document.querySelector("#marketMessage");
const marketStats = document.querySelector("#marketStats");
const marketChart = document.querySelector("#marketChart");
const chartEmpty = document.querySelector("#chartEmpty");
const chartSubtitle = document.querySelector("#chartSubtitle");
const marketSourceBadge = document.querySelector("#marketSourceBadge");
const lineageStrip = document.querySelector("#lineageStrip");
const barsTableBody = document.querySelector("#barsTableBody");
const barCount = document.querySelector("#barCount");

const newsList = document.querySelector("#newsList");
const newsSummary = document.querySelector("#newsSummary");
const pageInfo = document.querySelector("#pageInfo");
const pageSizeSelect = document.querySelector("#pageSizeSelect");
const prevPageButton = document.querySelector("#prevPageButton");
const nextPageButton = document.querySelector("#nextPageButton");
const refreshNewsButton = document.querySelector("#refreshNewsButton");
const pageNavButtons = document.querySelectorAll(".nav-button");
const reportFilterForm = document.querySelector("#reportFilterForm");
const reportStockCode = document.querySelector("#reportStockCode");
const reportYear = document.querySelector("#reportYear");
const reportSummary = document.querySelector("#reportSummary");
const reportList = document.querySelector("#reportList");
const reportPageInfo = document.querySelector("#reportPageInfo");
const prevReportPageButton = document.querySelector("#prevReportPageButton");
const nextReportPageButton = document.querySelector("#nextReportPageButton");
const refreshReportsButton = document.querySelector("#refreshReportsButton");
const refreshWatchlistButton = document.querySelector("#refreshWatchlistButton");
const watchlistForm = document.querySelector("#watchlistForm");
const watchlistSymbol = document.querySelector("#watchlistSymbol");
const priceMoveThreshold = document.querySelector("#priceMoveThreshold");
const volumeRatioThreshold = document.querySelector("#volumeRatioThreshold");
const watchlistMessage = document.querySelector("#watchlistMessage");
const watchlistList = document.querySelector("#watchlistList");
const runMonitorButton = document.querySelector("#runMonitorButton");
const refreshMonitorButton = document.querySelector("#refreshMonitorButton");
const monitorFilterForm = document.querySelector("#monitorFilterForm");
const monitorTradingDate = document.querySelector("#monitorTradingDate");
const monitorStatus = document.querySelector("#monitorStatus");
const monitorMessage = document.querySelector("#monitorMessage");
const monitorReportList = document.querySelector("#monitorReportList");

const state = {
  newsPage: 1,
  newsPageSize: Number(pageSizeSelect.value),
  reportPage: 1,
  reportPageSize: 20,
  marketRequestId: 0,
  marketAbortController: null,
  chart: null,
};

async function loadSecurityOptions() {
  try {
    const response = await fetch("/api/market/securities");
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "证券列表加载失败");
    const items = payload.items || [];
    symbolInput.innerHTML = items.length
      ? items.map((item) => `<option value="${escapeHtml(item.symbol)}">${escapeHtml(item.symbol)} · ${escapeHtml(item.name)} · ${escapeHtml(item.exchange)}</option>`).join("")
      : '<option value="" selected disabled>暂无可用证券</option>';
    if (items.some((item) => item.symbol === "600519.SH")) symbolInput.value = "600519.SH";
    if (items.length && marketForm.checkValidity()) loadMarket();
  } catch (error) {
    console.warn(error.message || "证券列表加载失败");
    symbolInput.innerHTML = '<option value="" selected disabled>证券列表加载失败</option>';
  }
}

function pad(value) {
  return String(value).padStart(2, "0");
}

function toDateInputValue(date) {
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
}

function initializeDateRange() {
  const end = new Date();
  const start = new Date(end);
  start.setDate(start.getDate() - 90);
  endDateInput.value = toDateInputValue(end);
  startDateInput.value = toDateInputValue(start);
}

function formatDate(value) {
  if (!value) return "未知日期";
  const date = new Date(`${String(value).slice(0, 10)}T00:00:00`);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("zh-CN", { year: "numeric", month: "2-digit", day: "2-digit" }).format(date);
}

function formatDateTime(value) {
  if (!value) return "未知时间";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "short" }).format(date);
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return Number(value).toLocaleString("zh-CN", { minimumFractionDigits: digits, maximumFractionDigits: digits });
}

function formatPercent(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return "—";
  return `${(Number(value) * 100).toFixed(2)}%`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function setMarketMessage(message, kind = "info") {
  marketMessage.textContent = message;
  marketMessage.className = `market-message ${kind}`;
}

function setMonitorMessage(element, message, kind = "info") {
  element.textContent = message;
  element.className = `monitor-message ${kind}`;
}

function setLoading(isLoading) {
  if (isLoading) {
    marketSourceBadge.textContent = "读取湖仓";
    setMarketMessage("正在读取 Silver/Gold 行情数据…", "info");
  }
}

function renderStats(summary) {
  const latest = summary.last_close;
  const cards = [
    ["最新收盘", formatNumber(latest), summary.recent_trading_date ? formatDate(summary.recent_trading_date) : ""],
    ["区间涨跌", formatPercent(summary.interval_return), `${formatDate(summary.start_date)} — ${formatDate(summary.end_date)}`],
    ["年化波动率", formatPercent(summary.annualized_volatility), "按日收益率估算"],
    ["停牌天数", `${summary.suspended_days ?? 0} 天`, summary.data_gap_note || "未发现停牌记录"],
  ];
  marketStats.innerHTML = cards.map(([label, value, note]) => `<div class="stat-card"><span>${escapeHtml(label)}</span><strong>${escapeHtml(value)}</strong><small>${escapeHtml(note)}</small></div>`).join("");
}

function renderLineage(summary, bars) {
  const latest = bars[bars.length - 1] || {};
  const first = bars[0] || {};
  marketSourceBadge.textContent = `${summary.symbol} · ${summary.exchange}`;
  lineageStrip.innerHTML = `
    <div><span>数据来源</span><strong>${escapeHtml(latest.provider || "内部湖仓")}</strong></div>
    <div><span>复权口径</span><strong>${escapeHtml(summary.adjustment_type || "—")}</strong></div>
    <div><span>可用时间</span><strong>${escapeHtml(formatDateTime(latest.available_at))}</strong></div>
    <div><span>写入时间</span><strong>${escapeHtml(formatDateTime(latest.ingested_at))}</strong></div>
    <div class="lineage-path"><span>Bronze 原始对象</span><strong title="${escapeHtml(first.raw_object_path || latest.raw_object_path || "—")}">${escapeHtml(first.raw_object_path || latest.raw_object_path || "—")}</strong></div>`;
}

function renderBarsTable(bars) {
  barCount.textContent = `${bars.length} 条记录`;
  if (!bars.length) {
    barsTableBody.innerHTML = '<tr><td colspan="7" class="table-empty">暂无日线数据</td></tr>';
    return;
  }
  barsTableBody.innerHTML = [...bars].reverse().map((bar) => {
    const flags = [bar.is_suspended ? "停牌" : "", bar.is_st ? "ST" : ""].filter(Boolean);
    return `<tr><td>${escapeHtml(formatDate(bar.trading_date))}</td><td>${formatNumber(bar.open)}</td><td>${formatNumber(bar.high)}</td><td>${formatNumber(bar.low)}</td><td class="${Number(bar.close) >= Number(bar.open) ? "up-text" : "down-text"}">${formatNumber(bar.close)}</td><td>${formatNumber(bar.volume, 0)}</td><td>${flags.length ? flags.map((flag) => `<span class="table-tag">${escapeHtml(flag)}</span>`).join("") : '<span class="muted">正常</span>'}</td></tr>`;
  }).join("");
}

function renderChart(summary) {
  const bars = [...(summary.bars || [])].sort((a, b) => String(a.trading_date).localeCompare(String(b.trading_date)));
  const hasData = bars.length > 0;
  chartEmpty.hidden = hasData;
  marketChart.classList.toggle("is-empty", !hasData);
  chartSubtitle.textContent = hasData ? `${formatDate(bars[0].trading_date)} — ${formatDate(bars[bars.length - 1].trading_date)} · ${bars.length} 个交易日` : "暂无数据";
  if (!hasData || typeof window.echarts === "undefined") {
    if (hasData && typeof window.echarts === "undefined") chartEmpty.textContent = "图表组件加载失败，可查看下方日线明细";
    if (state.chart) state.chart.clear();
    return;
  }
  if (!state.chart) state.chart = window.echarts.init(marketChart);
  const dates = bars.map((bar) => bar.trading_date);
  const candles = bars.map((bar) => [Number(bar.open), Number(bar.close), Number(bar.low), Number(bar.high)]);
  const volumes = bars.map((bar) => ({ value: Number(bar.volume || 0), itemStyle: { color: Number(bar.close) >= Number(bar.open) ? "#ef6a6a" : "#4aa38a" } }));
  state.chart.setOption({
    animation: false,
    tooltip: { trigger: "axis", axisPointer: { type: "cross" }, backgroundColor: "rgba(20, 31, 49, .94)", borderWidth: 0, textStyle: { color: "#fff" }, formatter: (params) => {
      const index = params[0]?.dataIndex ?? 0;
      const bar = bars[index];
      const flags = [bar.is_suspended ? "停牌" : "", bar.is_st ? "ST" : ""].filter(Boolean).join(" / ");
      return `<strong>${escapeHtml(formatDate(bar.trading_date))}</strong><br/>开盘 ${formatNumber(bar.open)}　收盘 ${formatNumber(bar.close)}<br/>最高 ${formatNumber(bar.high)}　最低 ${formatNumber(bar.low)}<br/>成交量 ${formatNumber(bar.volume, 0)}${flags ? `<br/>${escapeHtml(flags)}` : ""}`;
    } },
    axisPointer: { link: [{ xAxisIndex: "all" }] },
    grid: [{ left: 56, right: 22, top: 24, height: "58%" }, { left: 56, right: 22, top: "72%", height: "16%" }],
    xAxis: [{ type: "category", data: dates, boundaryGap: true, axisLine: { lineStyle: { color: "#d7dee8" } }, axisLabel: { color: "#768397", formatter: (value) => String(value).slice(5) } }, { type: "category", gridIndex: 1, data: dates, axisLabel: { show: false }, axisLine: { show: false } }],
    yAxis: [{ scale: true, splitLine: { lineStyle: { color: "#eef2f6" } }, axisLabel: { color: "#768397" } }, { scale: true, gridIndex: 1, splitLine: { show: false }, axisLabel: { color: "#768397" } }],
    dataZoom: [{ type: "inside", xAxisIndex: [0, 1], start: Math.max(0, 100 - (bars.length > 90 ? 70 : 100)), end: 100 }, { type: "slider", xAxisIndex: [0, 1], bottom: 4, height: 18, borderColor: "#d7dee8", fillerColor: "rgba(15, 118, 110, .14)", textStyle: { color: "#768397" } }],
    series: [{ name: "价格", type: "candlestick", data: candles, itemStyle: { color: "#ef6a6a", color0: "#4aa38a", borderColor: "#ef6a6a", borderColor0: "#4aa38a" } }, { name: "成交量", type: "bar", xAxisIndex: 1, yAxisIndex: 1, data: volumes }],
  }, true);
}

function renderMarketSummary(summary) {
  const bars = summary.bars || [];
  renderStats(summary);
  renderChart(summary);
  renderLineage(summary, bars);
  renderBarsTable(bars);
}

function clearMarket() {
  marketStats.innerHTML = "";
  lineageStrip.innerHTML = "";
  renderBarsTable([]);
  renderChart({ bars: [] });
}

async function loadMarket() {
  if (!marketForm.checkValidity()) return;
  const requestId = ++state.marketRequestId;
  if (state.marketAbortController) state.marketAbortController.abort();
  state.marketAbortController = new AbortController();
  setLoading(true);
  const params = new URLSearchParams({ symbol: symbolInput.value.trim(), start_date: startDateInput.value, end_date: endDateInput.value, adjustment_type: adjustmentInput.value });
  if (asOfInput.value) {
    const asOf = new Date(asOfInput.value);
    if (!Number.isNaN(asOf.getTime())) params.set("as_of", asOf.toISOString());
  }
  try {
    const response = await fetch(`/api/market?${params.toString()}`, { signal: state.marketAbortController.signal });
    const payload = await response.json().catch(() => ({}));
    if (requestId !== state.marketRequestId) return;
    if (!response.ok) throw new Error(payload.detail || `行情查询失败（${response.status}）`);
    const summary = payload.items?.[0];
    if (!summary) {
      clearMarket();
      marketSourceBadge.textContent = "暂无数据";
      setMarketMessage(payload.message || "湖仓中暂无符合条件的行情数据。", "warning");
      return;
    }
    renderMarketSummary(summary);
    setMarketMessage(summary.adjustment_note || "数据来自内部 Silver/Gold 湖仓。", "success");
  } catch (error) {
    if (error.name === "AbortError") return;
    clearMarket();
    marketSourceBadge.textContent = "查询失败";
    setMarketMessage(error.message || "行情查询失败，请稍后重试。", "error");
  } finally {
    if (requestId === state.marketRequestId) setLoading(false);
  }
}

function addMessage(role, text, toolResults = null) {
  const article = document.createElement("article");
  article.className = `message ${role}`;
  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.textContent = text;
  if (toolResults) {
    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.textContent = "查看工具数据";
    const pre = document.createElement("pre");
    pre.className = "tool-results";
    pre.textContent = JSON.stringify(toolResults, null, 2);
    details.append(summary, pre);
    bubble.appendChild(details);
  }
  article.appendChild(bubble);
  messages.appendChild(article);
  messages.scrollTop = messages.scrollHeight;
}

async function sendQuestion(question) {
  chatStatus.textContent = "思考中";
  chatStatus.classList.add("busy");
  sendButton.disabled = true;
  try {
    const response = await fetch("/api/chat", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ question }) });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || `请求失败（${response.status}）`);
    addMessage("assistant", payload.analysis || "没有返回分析结果。", payload.tool_results);
    const marketData = payload.tool_results?.data;
    if (marketData?.bars?.length) {
      if ([...symbolInput.options].some((option) => option.value === marketData.symbol)) symbolInput.value = marketData.symbol;
      renderMarketSummary(marketData);
      setMarketMessage(marketData.adjustment_note || "行情结果已同步到图表。", "success");
    }
  } catch (error) {
    addMessage("error", error.message || "聊天请求失败。");
  } finally {
    chatStatus.textContent = "就绪";
    chatStatus.classList.remove("busy");
    sendButton.disabled = false;
    questionInput.focus();
  }
}

function formatNewsExcerpt(item, maxLength = 180) {
  const text = String(item.content || item.summary || "").trim().replace(/\s+/g, " ");
  return text.length > maxLength ? `${text.slice(0, maxLength)}…` : text;
}

function renderNewsItems(items) {
  if (!items.length) { newsList.innerHTML = '<div class="empty-state">当前湖仓暂无可展示的新闻。</div>'; return; }
  newsList.innerHTML = items.map((item) => {
    const tags = [...(item.symbols || []), ...(item.entities || []).slice(0, 3)].map((tag) => `<span class="tag">${escapeHtml(tag)}</span>`).join("");
    const href = item.url ? `<a href="${escapeHtml(item.url)}" target="_blank" rel="noreferrer">${escapeHtml(item.title)}</a>` : escapeHtml(item.title);
    return `<article class="news-card"><h3>${href}</h3><p>${escapeHtml(formatNewsExcerpt(item))}</p><div class="news-time"><span>${escapeHtml(item.source_name || item.provider || "未知来源")}</span><span>${escapeHtml(formatDateTime(item.published_at))}</span></div><div class="news-tags">${tags}</div></article>`;
  }).join("");
}

async function loadNews() {
  refreshNewsButton.disabled = true;
  newsSummary.textContent = "加载中…";
  try {
    const params = new URLSearchParams({ page: String(state.newsPage), page_size: String(state.newsPageSize) });
    const response = await fetch(`/api/news?${params.toString()}`);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || `新闻加载失败（${response.status}）`);
    renderNewsItems(payload.items || []);
    newsSummary.textContent = `共 ${payload.total || 0} 条新闻`;
    pageInfo.textContent = `第 ${payload.page || state.newsPage} / ${payload.total_pages || 1} 页`;
    prevPageButton.disabled = (payload.page || state.newsPage) <= 1;
    nextPageButton.disabled = !payload.has_next;
  } catch (error) {
    newsList.innerHTML = `<div class="empty-state">${escapeHtml(error.message || "新闻加载失败")}</div>`;
    newsSummary.textContent = "加载失败";
    nextPageButton.disabled = true;
  } finally { refreshNewsButton.disabled = false; }
}

function renderReports(items) {
  if (!items.length) {
    reportList.innerHTML = '<div class="empty-state">当前筛选条件下没有可展示的年报。</div>';
    return;
  }
  reportList.innerHTML = items.map((report) => `
    <article class="report-card" data-report-id="${escapeHtml(report.report_id)}">
      <h3>${escapeHtml(report.title)}</h3>
      <div class="report-meta"><span>${escapeHtml(report.stock_code)} · ${escapeHtml(report.company_name || "公司名称未知")}</span><span>${escapeHtml(String(report.report_year))} 年报</span><span>${escapeHtml(formatDateTime(report.announcement_time))}</span><span>${escapeHtml(report.extraction_status || "未知状态")}</span></div>
      <div class="report-actions"><a href="/api/reports/${encodeURIComponent(report.report_id)}/download" target="_blank" rel="noreferrer">打开 PDF</a>${report.page_count > 0 ? '<button type="button" data-action="read-page">查看首页文本</button>' : ""}</div>
      <div class="report-page-text" hidden></div>
    </article>`).join("");
}

async function loadReports() {
  refreshReportsButton.disabled = true;
  reportSummary.textContent = "加载中…";
  try {
    const params = new URLSearchParams({ page: String(state.reportPage), page_size: String(state.reportPageSize) });
    if (reportStockCode.value.trim()) params.set("stock_code", reportStockCode.value.trim().toUpperCase());
    if (reportYear.value) params.set("report_year", reportYear.value);
    const response = await fetch(`/api/reports?${params.toString()}`);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || `财报加载失败（${response.status}）`);
    renderReports(payload.items || []);
    reportSummary.textContent = `共 ${payload.total || 0} 份年报`;
    reportPageInfo.textContent = `第 ${payload.page || state.reportPage} 页`;
    prevReportPageButton.disabled = (payload.page || state.reportPage) <= 1;
    nextReportPageButton.disabled = !payload.has_next;
  } catch (error) {
    reportList.innerHTML = `<div class="empty-state">${escapeHtml(error.message || "财报加载失败")}</div>`;
    reportSummary.textContent = "加载失败";
    prevReportPageButton.disabled = true;
    nextReportPageButton.disabled = true;
  } finally {
    refreshReportsButton.disabled = false;
  }
}

async function showReportPage(button) {
  const card = button.closest(".report-card");
  const textBox = card.querySelector(".report-page-text");
  if (!textBox.hidden) {
    textBox.hidden = true;
    button.textContent = "查看首页文本";
    return;
  }
  button.disabled = true;
  button.textContent = "读取中…";
  try {
    const reportId = card.dataset.reportId;
    const response = await fetch(`/api/reports/${encodeURIComponent(reportId)}/pages/1`);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "年报文本读取失败");
    textBox.textContent = payload.text || "该页没有可提取的文本。";
    textBox.hidden = false;
    button.textContent = "收起首页文本";
  } catch (error) {
    textBox.textContent = error.message || "年报文本读取失败";
    textBox.hidden = false;
    button.textContent = "收起提示";
  } finally {
    button.disabled = false;
  }
}

function renderWatchlist(items) {
  if (!items.length) {
    watchlistList.innerHTML = '<div class="empty-state">监护清单为空。加入一只 A 股后即可生成收盘后日报。</div>';
    return;
  }
  watchlistList.innerHTML = items.map((item) => `
    <article class="watchlist-card ${item.enabled ? "" : "is-disabled"}" data-symbol="${escapeHtml(item.symbol)}">
      <div class="watchlist-title"><div><strong>${escapeHtml(item.symbol)}</strong><span>${escapeHtml(item.name)}</span></div><span class="status-pill">${item.enabled ? "已启用" : "已暂停"}</span></div>
      <div class="watchlist-meta"><span>${escapeHtml(item.exchange)}</span><span>涨跌 ${formatPercent(item.thresholds.price_move)}</span><span>量能 ${formatNumber(item.thresholds.volume_ratio)}x</span><span>v${escapeHtml(item.version)}</span></div>
      ${item.note ? `<p>${escapeHtml(item.note)}</p>` : ""}
      <div class="watchlist-actions"><button type="button" data-action="edit">编辑阈值</button><button type="button" data-action="toggle">${item.enabled ? "暂停" : "启用"}</button><button type="button" data-action="delete">移除</button></div>
    </article>`).join("");
}

async function loadWatchlist() {
  refreshWatchlistButton.disabled = true;
  try {
    const response = await fetch("/api/watchlist");
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "监护清单加载失败");
    renderWatchlist(payload || []);
  } catch (error) {
    watchlistList.innerHTML = `<div class="empty-state">${escapeHtml(error.message || "监护清单加载失败")}</div>`;
  } finally {
    refreshWatchlistButton.disabled = false;
  }
}

function renderMonitorRecommendation(title, recommendation) {
  if (!recommendation) return "";
  const stanceLabels = { bullish: "偏多", neutral: "中性", bearish: "偏空", avoid: "回避" };
  return `<div class="recommendation"><div><span>${escapeHtml(title)}</span><strong class="stance-${escapeHtml(recommendation.stance)}">${escapeHtml(stanceLabels[recommendation.stance] || recommendation.stance)}</strong><small>置信度 ${formatPercent(recommendation.confidence)}</small></div><p>${escapeHtml(recommendation.rationale)}</p>${recommendation.entry_condition ? `<small>触发：${escapeHtml(recommendation.entry_condition)}</small>` : ""}${recommendation.invalidation_condition ? `<small>失效：${escapeHtml(recommendation.invalidation_condition)}</small>` : ""}</div>`;
}

function renderMonitorReports(items) {
  if (!items.length) {
    monitorReportList.innerHTML = '<div class="empty-state">暂无监测报告。先维护监护清单，再点击“立即运行”。</div>';
    return;
  }
  monitorReportList.innerHTML = items.map((report) => {
    const events = (report.events || []).map((event) => `<span class="tag">${escapeHtml(event.description)}</span>`).join("");
    const citations = (report.citations || []).map((citation) => `<li><span>[${citation.citation_id}] ${escapeHtml(citation.title)}</span>${citation.url ? `<a href="${escapeHtml(citation.url)}" target="_blank" rel="noreferrer">查看</a>` : ""}</li>`).join("");
    return `<article class="monitor-report-card">
      <div class="monitor-report-heading"><div><strong>${escapeHtml(report.symbol)} · ${escapeHtml(report.name)}</strong><span>${escapeHtml(formatDate(report.trading_date))} · ${escapeHtml(report.status)} · ${escapeHtml(report.data_status)}</span></div><span class="status-pill ${report.status === "degraded" ? "busy" : ""}">${escapeHtml(report.status)}</span></div>
      <p>${escapeHtml(report.analysis || "无分析文本")}</p>
      <div class="monitor-events">${events || '<span class="muted">未触发确定性事件</span>'}</div>
      <div class="recommendation-grid">${renderMonitorRecommendation("5 个交易日", report.recommendation_5d)}${renderMonitorRecommendation("20 个交易日", report.recommendation_20d)}</div>
      ${(report.missing_data || []).length ? `<div class="degraded-note">数据限制：${escapeHtml(report.missing_data.join("；"))}</div>` : ""}
      <details><summary>证据与引用（${(report.citations || []).length}）</summary><ul class="citation-list">${citations}</ul></details>
    </article>`;
  }).join("");
}

async function loadMonitorReports() {
  refreshMonitorButton.disabled = true;
  setMonitorMessage(monitorMessage, "正在读取监测报告。");
  try {
    const params = new URLSearchParams();
    if (monitorTradingDate.value) params.set("trading_date", monitorTradingDate.value);
    if (monitorStatus.value) params.set("status", monitorStatus.value);
    const response = await fetch(`/api/monitor/reports?${params.toString()}`);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "监测报告加载失败");
    renderMonitorReports(payload.items || []);
    setMonitorMessage(monitorMessage, `共 ${payload.total || 0} 条监测报告。`, "success");
  } catch (error) {
    monitorReportList.innerHTML = `<div class="empty-state">${escapeHtml(error.message || "监测报告加载失败")}</div>`;
    setMonitorMessage(monitorMessage, error.message || "监测报告加载失败", "error");
  } finally {
    refreshMonitorButton.disabled = false;
  }
}

async function pollMonitorRun(runId) {
  for (let attempt = 0; attempt < 240; attempt += 1) {
    await new Promise((resolve) => setTimeout(resolve, 5000));
    const response = await fetch(`/api/monitor/runs/${encodeURIComponent(runId)}`);
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "运行状态读取失败");
    setMonitorMessage(monitorMessage, `监测任务 ${payload.status}，请稍候…`, payload.status === "failed" ? "error" : "info");
    if (["completed", "degraded", "skipped", "failed"].includes(payload.status)) {
      await loadMonitorReports();
      return;
    }
  }
  throw new Error("监测任务等待超时，请稍后刷新收件箱。");
}

pageNavButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const pageId = button.dataset.page;
    document.querySelectorAll(".page-view").forEach((page) => {
      const active = page.id === pageId;
      page.hidden = !active;
      page.classList.toggle("active", active);
    });
    pageNavButtons.forEach((item) => {
      const active = item === button;
      item.classList.toggle("active", active);
      if (active) item.setAttribute("aria-current", "page");
      else item.removeAttribute("aria-current");
    });
    if (pageId === "researchPage") loadReports();
    if (pageId === "monitorPage") { loadWatchlist(); loadMonitorReports(); }
    requestAnimationFrame(() => state.chart?.resize());
  });
});

watchlistForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const button = watchlistForm.querySelector("button[type='submit']");
  button.disabled = true;
  try {
    const response = await fetch("/api/watchlist", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ symbol: watchlistSymbol.value.trim().toUpperCase(), thresholds: { price_move: Number(priceMoveThreshold.value), volume_ratio: Number(volumeRatioThreshold.value) } }) });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "加入监护清单失败");
    watchlistSymbol.value = "";
    setMonitorMessage(watchlistMessage, `${payload.symbol} 已加入监护清单。`, "success");
    await loadWatchlist();
  } catch (error) {
    setMonitorMessage(watchlistMessage, error.message || "加入监护清单失败", "error");
  } finally {
    button.disabled = false;
  }
});

watchlistList.addEventListener("click", async (event) => {
  const button = event.target.closest("button[data-action]");
  if (!button) return;
  const card = button.closest(".watchlist-card");
  const symbol = card.dataset.symbol;
  button.disabled = true;
  try {
    if (button.dataset.action === "delete") {
      const response = await fetch(`/api/watchlist/${encodeURIComponent(symbol)}`, { method: "DELETE" });
      if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || "移除失败");
    } else if (button.dataset.action === "edit") {
      const priceMove = window.prompt("单日涨跌阈值（例如 0.03）", "0.03");
      if (priceMove === null) { button.disabled = false; return; }
      const volumeRatio = window.prompt("成交量倍数阈值（例如 1.8）", "1.8");
      if (volumeRatio === null) { button.disabled = false; return; }
      const response = await fetch(`/api/watchlist/${encodeURIComponent(symbol)}`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ thresholds: { price_move: Number(priceMove), volume_ratio: Number(volumeRatio) } }) });
      if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || "阈值更新失败");
    } else {
      const enabled = button.textContent.trim() === "启用";
      const response = await fetch(`/api/watchlist/${encodeURIComponent(symbol)}`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ enabled }) });
      if (!response.ok) throw new Error((await response.json().catch(() => ({}))).detail || "更新失败");
    }
    await loadWatchlist();
  } catch (error) {
    setMonitorMessage(watchlistMessage, error.message || "监护清单更新失败", "error");
    button.disabled = false;
  }
});

refreshWatchlistButton.addEventListener("click", loadWatchlist);
refreshMonitorButton.addEventListener("click", loadMonitorReports);
monitorFilterForm.addEventListener("submit", (event) => { event.preventDefault(); loadMonitorReports(); });
runMonitorButton.addEventListener("click", async () => {
  runMonitorButton.disabled = true;
  try {
    const response = await fetch("/api/monitor/runs", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({}) });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || "监测任务提交失败");
    setMonitorMessage(monitorMessage, `监测任务已提交：${payload.run_id}`, "success");
    await pollMonitorRun(payload.run_id);
  } catch (error) {
    setMonitorMessage(monitorMessage, error.message || "监测任务提交失败", "error");
  } finally {
    runMonitorButton.disabled = false;
  }
});

reportFilterForm.addEventListener("submit", (event) => {
  event.preventDefault();
  state.reportPage = 1;
  loadReports();
});
refreshReportsButton.addEventListener("click", loadReports);
prevReportPageButton.addEventListener("click", () => { if (state.reportPage > 1) { state.reportPage -= 1; loadReports(); } });
nextReportPageButton.addEventListener("click", () => { state.reportPage += 1; loadReports(); });
reportList.addEventListener("click", (event) => {
  const button = event.target.closest('[data-action="read-page"]');
  if (button) showReportPage(button);
});

marketForm.addEventListener("submit", (event) => event.preventDefault());
marketForm.addEventListener("change", (event) => {
  if (event.target.matches("select, input")) loadMarket();
});
chatForm.addEventListener("submit", (event) => { event.preventDefault(); const question = questionInput.value.trim(); if (!question) return; addMessage("user", question); questionInput.value = ""; sendQuestion(question); });
questionInput.addEventListener("keydown", (event) => { if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) chatForm.requestSubmit(); });
refreshNewsButton.addEventListener("click", loadNews);
pageSizeSelect.addEventListener("change", () => { state.newsPage = 1; state.newsPageSize = Number(pageSizeSelect.value); loadNews(); });
prevPageButton.addEventListener("click", () => { if (state.newsPage > 1) { state.newsPage -= 1; loadNews(); } });
nextPageButton.addEventListener("click", () => { state.newsPage += 1; loadNews(); });
window.addEventListener("resize", () => state.chart?.resize());

initializeDateRange();
loadSecurityOptions();
loadNews();
for (let year = new Date().getFullYear(); year >= new Date().getFullYear() - 10; year -= 1) {
  const option = document.createElement("option");
  option.value = String(year);
  option.textContent = `${year} 年`;
  reportYear.append(option);
}
