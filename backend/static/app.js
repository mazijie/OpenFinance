const chatForm = document.querySelector("#chatForm");
const questionInput = document.querySelector("#questionInput");
const sendButton = document.querySelector("#sendButton");
const messages = document.querySelector("#messages");
const chatStatus = document.querySelector("#chatStatus");
const systemStatus = document.querySelector("#systemStatus");
const chatThreadTabs = document.querySelector("#chatThreadTabs");
const newChatThreadButton = document.querySelector("#newChatThreadButton");
let chatProgress = null;
let chatProgressTitle = null;
let chatProgressList = null;
let chatElapsed = null;

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
const newAgentThreadButton = document.querySelector("#newAgentThreadButton");
const agentThreadList = document.querySelector("#agentThreadList");
const agentThreadTitle = document.querySelector("#agentThreadTitle");
const agentRunStatus = document.querySelector("#agentRunStatus");
const cancelAgentTurnButton = document.querySelector("#cancelAgentTurnButton");
const agentToolTimeline = document.querySelector("#agentToolTimeline");
const agentMessages = document.querySelector("#agentMessages");
const agentComposer = document.querySelector("#agentComposer");
const agentQuestion = document.querySelector("#agentQuestion");
const agentSendButton = document.querySelector("#agentSendButton");
const deepResearchMode = document.querySelector("#deepResearchMode");
const inspectorContent = document.querySelector("#inspectorContent");
const refreshInspectorButton = document.querySelector("#refreshInspectorButton");
const manageWebDomainsButton = document.querySelector("#manageWebDomainsButton");
const modelSettingsDetails = document.querySelector("#modelSettingsDetails");
const modelSettingsForm = document.querySelector("#modelSettingsForm");
const llmApiKeyInput = document.querySelector("#llmApiKey");
const llmModelSelect = document.querySelector("#llmModel");
const modelSettingsStatus = document.querySelector("#modelSettingsStatus");
const clearLlmKeyButton = document.querySelector("#clearLlmKeyButton");

const LLM_KEY_STORAGE = "openfinance.llmApiKey";
const LLM_MODEL_STORAGE = "openfinance.llmModel";
const CHAT_THREAD_STORAGE = "openfinance.chatThreadId";
const DEFAULT_LLM_MODEL = "deepseek-flash";

const state = {
  newsPage: 1,
  newsPageSize: Number(pageSizeSelect.value),
  reportPage: 1,
  reportPageSize: 20,
  marketRequestId: 0,
  marketAbortController: null,
  chart: null,
  agentThreads: [],
  agentThreadId: null,
  agentTurnId: null,
  agentStreaming: false,
  chatThreads: [],
  chatThreadId: null,
  chatStreaming: false,
  chatStartedAt: null,
  chatProgressTimer: null,
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

function sessionValue(key) {
  try {
    return window.sessionStorage.getItem(key) || "";
  } catch {
    return "";
  }
}

function setSessionValue(key, value) {
  try {
    if (value) window.sessionStorage.setItem(key, value);
    else window.sessionStorage.removeItem(key);
  } catch {
    /* Private browsing can disable storage; the in-memory form still works. */
  }
}

function getLlmConfig() {
  return {
    apiKey: llmApiKeyInput.value.trim(),
    model: llmModelSelect.value || DEFAULT_LLM_MODEL,
  };
}

function requireLlmConfig() {
  const config = getLlmConfig();
  if (!config.apiKey) {
    if (modelSettingsDetails) modelSettingsDetails.open = true;
    window.setTimeout(() => llmApiKeyInput.focus(), 0);
    throw new Error("请先在左侧填写 DeepSeek API Key。");
  }
  return config;
}

function llmHeaders(extra = {}) {
  const config = requireLlmConfig();
  return {
    ...extra,
    "X-LLM-API-Key": config.apiKey,
    "X-LLM-Model": config.model,
  };
}

function renderLlmSettingsStatus() {
  const config = getLlmConfig();
  const ready = Boolean(config.apiKey);
  modelSettingsStatus.textContent = ready ? `已配置 · ${config.model}` : "未配置 Key";
  modelSettingsStatus.classList.toggle("ready", ready);
}

function loadLlmSettings() {
  llmApiKeyInput.value = sessionValue(LLM_KEY_STORAGE);
  const savedModel = sessionValue(LLM_MODEL_STORAGE);
  if ([...llmModelSelect.options].some((option) => option.value === savedModel)) {
    llmModelSelect.value = savedModel;
  }
  renderLlmSettingsStatus();
}

async function apiJson(url, options = {}) {
  const response = await fetch(url, options);
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(payload.detail || `请求失败（${response.status}）`);
  return payload;
}

async function loadAgentThreads(preferredThreadId = null) {
  const items = await apiJson("/api/agent/threads");
  if (!items.length) {
    const thread = await apiJson("/api/agent/threads", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({}) });
    state.agentThreads = [thread];
  } else {
    state.agentThreads = items;
  }
  const resolvedId = preferredThreadId || state.agentThreadId || state.agentThreads[0]?.thread_id;
  renderAgentThreads();
  if (resolvedId) await selectAgentThread(resolvedId);
}

function renderAgentThreads() {
  agentThreadList.innerHTML = state.agentThreads.map((thread) => `
    <article class="agent-thread-row" data-thread-id="${escapeHtml(thread.thread_id)}">
      <button class="agent-thread-item ${thread.thread_id === state.agentThreadId ? "active" : ""}" type="button" data-action="open">
        <strong>${escapeHtml(thread.title)}</strong>
        <span>${escapeHtml(formatDateTime(thread.last_message_at || thread.updated_at))}${thread.unread_count ? ` · ${thread.unread_count} 条未读` : ""}</span>
      </button>
      <button type="button" data-action="archive" title="归档线程">×</button>
    </article>`).join("");
}

async function selectAgentThread(threadId) {
  state.agentThreadId = threadId;
  const thread = state.agentThreads.find((item) => item.thread_id === threadId);
  agentThreadTitle.textContent = thread?.title || "研究对话";
  renderAgentThreads();
  await loadAgentMessages();
}

function renderAgentMessage(message) {
  const citations = message.citations || [];
  const report = message.kind === "monitor_report" ? message.payload?.report : null;
  const approval = message.kind === "approval" ? message.payload?.approval : null;
  const reportHtml = report ? `<div class="agent-monitor-card"><strong>${escapeHtml(report.symbol || "")} ${escapeHtml(report.name || "")} 监测报告</strong><p>${escapeHtml(report.analysis || "无分析文本")}</p><button type="button" class="secondary-button" data-action="open-monitor-dashboard">查看监测仪表盘</button></div>` : "";
  const approvalActions = !approval?.status || approval.status === "pending" ? '<div class="approval-actions"><button type="button" class="primary-button" data-decision="approve">批准</button><button type="button" class="secondary-button" data-decision="reject">拒绝</button></div>' : "";
  const approvalHtml = approval ? `<div class="approval-card" data-approval-id="${escapeHtml(approval.approval_id)}"><h4>需要确认：${escapeHtml(approval.tool_name)}</h4><p>${escapeHtml(approval.summary || JSON.stringify(approval.args || {}))}</p>${approvalActions}</div>` : "";
  const citationHtml = citations.length ? `<details class="agent-citations"><summary>来源与引用（${citations.length}）</summary><ol>${citations.map((citation) => `<li>[${citation.citation_id}] ${citation.url ? `<a href="${escapeHtml(citation.url)}" target="_blank" rel="noreferrer">${escapeHtml(citation.title)}</a>` : escapeHtml(citation.title)}${citation.locator ? ` · ${escapeHtml(citation.locator)}` : ""}</li>`).join("")}</ol></details>` : "";
  return `<article class="agent-message ${escapeHtml(message.role)} ${escapeHtml(message.kind)}"><div class="agent-bubble">${escapeHtml(message.content)}${reportHtml}${approvalHtml}${citationHtml}</div></article>`;
}

async function loadAgentMessages() {
  if (!state.agentThreadId) return;
  try {
    const items = await apiJson(`/api/agent/threads/${encodeURIComponent(state.agentThreadId)}/messages`);
    agentMessages.innerHTML = items.length
      ? items.map(renderAgentMessage).join("")
      : '<div class="empty-state">开始一段研究对话。Agent 会按需调用行情、新闻、年报和白名单网页。</div>';
    agentMessages.scrollTop = agentMessages.scrollHeight;
    const latestTurn = items.filter((item) => item.turn_id).at(-1)?.turn_id;
    if (latestTurn) loadInspector(latestTurn);
  } catch (error) {
    agentMessages.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  }
}

function setAgentActivity(text, kind = "running") {
  const item = document.createElement("span");
  item.className = `agent-tool-item ${kind}`;
  item.textContent = text;
  agentToolTimeline.appendChild(item);
  return item;
}

function appendAgentApproval(payload) {
  const card = document.createElement("article");
  card.className = "approval-card";
  card.dataset.approvalId = payload.approval_id;
  card.innerHTML = `<h4>需要确认：${escapeHtml(payload.tool_name)}</h4><p>${escapeHtml(payload.summary || JSON.stringify(payload.args || {}))}</p><div class="approval-actions"><button type="button" class="primary-button" data-decision="approve">批准</button><button type="button" class="secondary-button" data-decision="reject">拒绝</button></div>`;
  agentMessages.appendChild(card);
  agentMessages.scrollTop = agentMessages.scrollHeight;
}

async function submitApproval(approvalId, approved, card) {
  if (!state.agentThreadId) return;
  const buttons = card.querySelectorAll("button");
  buttons.forEach((button) => { button.disabled = true; });
  let headers;
  try {
    headers = llmHeaders({ "Content-Type": "application/json" });
  } catch (error) {
    addAgentSystemMessage(error.message);
    buttons.forEach((button) => { button.disabled = false; });
    return;
  }
  const response = await fetch(`/api/agent/threads/${encodeURIComponent(state.agentThreadId)}/approvals/${encodeURIComponent(approvalId)}`, {
    method: "POST",
    headers,
    body: JSON.stringify({ approved }),
  });
  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    addAgentSystemMessage(error.detail || "审批失败");
    buttons.forEach((button) => { button.disabled = false; });
    return;
  }
  card.remove();
  await consumeAgentEvents(response, true);
}

function addAgentSystemMessage(text) {
  const article = document.createElement("article");
  article.className = "agent-message system";
  article.innerHTML = `<div class="agent-bubble">${escapeHtml(text)}</div>`;
  agentMessages.appendChild(article);
  agentMessages.scrollTop = agentMessages.scrollHeight;
}

async function parseSSEStream(response, onEvent) {
  if (!response.body) throw new Error("浏览器不支持流式响应");
  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    let boundary;
    while ((boundary = buffer.indexOf("\n\n")) >= 0) {
      const block = buffer.slice(0, boundary);
      buffer = buffer.slice(boundary + 2);
      const data = block.split("\n").filter((line) => line.startsWith("data:" )).map((line) => line.slice(5).trim()).join("\n");
      if (data) {
        try { onEvent(JSON.parse(data)); } catch { /* Ignore incomplete non-JSON frames. */ }
      }
    }
  }
}

async function consumeAgentEvents(response, resume = false) {
  state.agentStreaming = true;
  agentSendButton.disabled = true;
  cancelAgentTurnButton.hidden = false;
  agentRunStatus.textContent = resume ? "继续研究" : "研究中";
  agentRunStatus.classList.add("busy");
  let streamingBubble = null;
  const toolItems = new Map();
  await parseSSEStream(response, (event) => {
    const type = event.event_type;
    const payload = event.payload || {};
    if (type === "turn.started") {
      state.agentTurnId = payload.turn_id;
      const article = document.createElement("article");
      article.className = "agent-message assistant streaming";
      article.innerHTML = '<div class="agent-bubble"></div>';
      agentMessages.appendChild(article);
      streamingBubble = article.querySelector(".agent-bubble");
    } else if (type === "tool.started") {
      const item = setAgentActivity(`调用 ${payload.tool_name}`, "running");
      toolItems.set(payload.call_id, item);
    } else if (type === "tool.completed") {
      const item = toolItems.get(payload.call_id);
      if (item) {
        item.className = `agent-tool-item ${payload.error ? "error" : "done"}`;
        item.textContent = `${payload.tool_name}${payload.error ? " 失败" : ` · ${payload.duration_ms || 0}ms`}`;
      }
    } else if (type === "approval.required") {
      window.setTimeout(() => appendAgentApproval(payload), 0);
      agentRunStatus.textContent = "等待确认";
    } else if (type === "message.delta" && streamingBubble) {
      streamingBubble.textContent += payload.text || "";
      agentMessages.scrollTop = agentMessages.scrollHeight;
    } else if (type === "message.completed" && streamingBubble) {
      streamingBubble.parentElement?.classList.remove("streaming");
    } else if (type === "monitor.queued") {
      setAgentActivity("监测任务已提交", "done");
    } else if (type === "turn.failed") {
      addAgentSystemMessage(payload.error || "Agent 运行失败");
    } else if (type === "turn.cancelled") {
      addAgentSystemMessage("本轮研究已停止");
    }
  });
  state.agentStreaming = false;
  agentSendButton.disabled = false;
  cancelAgentTurnButton.hidden = true;
  agentRunStatus.classList.remove("busy");
  agentRunStatus.textContent = "就绪";
  await loadAgentThreads(state.agentThreadId);
}

async function submitAgentTurn(question) {
  if (!state.agentThreadId) throw new Error("请先选择研究线程");
  const headers = llmHeaders({ "Content-Type": "application/json" });
  const response = await fetch(`/api/agent/threads/${encodeURIComponent(state.agentThreadId)}/turns`, {
    method: "POST",
    headers,
    body: JSON.stringify({ question, mode: deepResearchMode.checked ? "deep" : "standard" }),
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.detail || "研究请求失败");
  }
  agentMessages.insertAdjacentHTML("beforeend", renderAgentMessage({ role: "user", kind: "text", content: question, citations: [], payload: {} }));
  agentMessages.scrollTop = agentMessages.scrollHeight;
  await consumeAgentEvents(response);
}

async function loadInspector(turnId = state.agentTurnId) {
  if (!turnId) return;
  try {
    const detail = await apiJson(`/api/agent/runs/${encodeURIComponent(turnId)}`);
    const toolCalls = detail.tool_calls || [];
    inspectorContent.innerHTML = `
      <section class="inspector-section"><h4>本轮状态</h4>
        <div class="inspector-row"><span>模式</span><strong>${escapeHtml(detail.turn.mode)}</strong></div>
        <div class="inspector-row"><span>状态</span><strong>${escapeHtml(detail.turn.status)}</strong></div>
        <div class="inspector-row"><span>检查点</span><strong>${escapeHtml(detail.turn.checkpoint_id || "运行中")}</strong></div>
        <div class="inspector-row"><span>工具调用</span><strong>${escapeHtml(String(detail.turn.stats?.tool_calls_used || toolCalls.length))}</strong></div>
      </section>
      <section class="inspector-section"><h4>工具调用</h4>
        ${toolCalls.length ? toolCalls.map((call) => `<div class="inspector-row"><span>${escapeHtml(call.tool_name)}</span><strong>${escapeHtml(call.status)}</strong></div>`).join("") : '<div class="muted">本轮尚未调用工具</div>'}
      </section>
      <section class="inspector-section"><h4>审批</h4>
        ${(detail.approvals || []).length ? detail.approvals.map((approval) => `<div class="inspector-row"><span>${escapeHtml(approval.tool_name)}</span><strong>${escapeHtml(approval.status)}</strong></div>`).join("") : '<div class="muted">无审批操作</div>'}
      </section>`;
  } catch (error) {
    inspectorContent.innerHTML = `<div class="empty-state">${escapeHtml(error.message)}</div>`;
  }
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
    marketSourceBadge.textContent = "正在查询";
    setMarketMessage("正在获取最新行情数据…", "info");
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
  const adjustmentLabels = { qfq: "前复权", hfq: "后复权", none: "不复权" };
  const latestDate = summary.recent_trading_date || latest.trading_date;
  const updateTime = latest.available_at || latest.ingested_at;
  marketSourceBadge.textContent = `${summary.symbol} · ${summary.exchange}`;
  lineageStrip.innerHTML = `
    <div><span>最新交易日</span><strong>${escapeHtml(latestDate ? formatDate(latestDate) : "—")}</strong></div>
    <div><span>复权方式</span><strong>${escapeHtml(adjustmentLabels[summary.adjustment_type] || "不复权")}</strong></div>
    <div><span>数据更新</span><strong>${escapeHtml(updateTime ? formatDateTime(updateTime) : "—")}</strong></div>
    <div><span>数据状态</span><strong>${escapeHtml(summary.data_gap_note ? "部分数据缺失" : "数据完整")}</strong></div>`;
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
      setMarketMessage(payload.message || "当前条件下暂无行情数据。", "warning");
      return;
    }
    renderMarketSummary(summary);
    setMarketMessage("行情数据已更新，可在图表和明细中查看。", "success");
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

function titleFromQuestion(question) {
  const normalized = String(question || "").trim().replace(/\s+/g, " ");
  return normalized.length > 24 ? `${normalized.slice(0, 24)}…` : normalized || "新对话";
}

function activeChatThread() {
  return state.chatThreads.find((thread) => thread.thread_id === state.chatThreadId) || null;
}

function renderChatThreadTabs() {
  if (!chatThreadTabs) return;
  chatThreadTabs.innerHTML = state.chatThreads.map((thread) => `
    <div class="chat-thread-tab ${thread.thread_id === state.chatThreadId ? "active" : ""}" role="presentation">
      <button type="button" role="tab" aria-selected="${thread.thread_id === state.chatThreadId}" data-chat-thread="${escapeHtml(thread.thread_id)}" title="${escapeHtml(thread.title)}（双击重命名）">${escapeHtml(thread.title)}</button>
      <button type="button" class="chat-thread-close" data-archive-chat="${escapeHtml(thread.thread_id)}" title="归档对话">×</button>
    </div>
  `).join("");
  chatThreadTabs.querySelector(".chat-thread-tab.active")?.scrollIntoView({ block: "nearest", inline: "nearest" });
}

function formatEventDuration(events) {
  if (!events?.length) return "";
  const first = new Date(events[0].created_at || 0).getTime();
  const last = new Date(events.at(-1).created_at || 0).getTime();
  if (!Number.isFinite(first) || !Number.isFinite(last) || last < first) return "";
  const seconds = (last - first) / 1000;
  return seconds < 10 ? `${seconds.toFixed(1)}s` : `${Math.round(seconds)}s`;
}

function buildHistoricalProgress(events) {
  if (!events?.length) return null;
  const toolEvents = events.filter((event) => event.event_type === "tool.completed");
  const terminal = [...events].reverse().find((event) => ["turn.completed", "turn.failed", "turn.cancelled"].includes(event.event_type));
  const budget = events.find((event) => event.event_type === "step.completed" && event.payload?.status === "budget_exhausted");
  const article = document.createElement("article");
  article.className = "message progress";
  const details = document.createElement("details");
  details.className = "chat-progress completed";
  const summary = document.createElement("summary");
  summary.className = "chat-progress-heading";
  const title = document.createElement("strong");
  title.textContent = terminal?.event_type === "turn.failed"
    ? "运行记录 · 失败"
    : terminal?.event_type === "turn.cancelled"
      ? "运行记录 · 已停止"
      : `运行记录 · ${toolEvents.length} 次工具调用`;
  const meta = document.createElement("span");
  meta.textContent = formatEventDuration(events);
  summary.append(title, meta);
  const list = document.createElement("ol");
  if (toolEvents.length) {
    toolEvents.forEach((event) => {
      const payload = event.payload || {};
      const item = document.createElement("li");
      item.className = payload.error ? "error" : "done";
      const content = document.createElement("div");
      const label = document.createElement("strong");
      label.textContent = describeChatTool(payload).label;
      content.appendChild(label);
      if (payload.error) {
        const note = document.createElement("small");
        note.textContent = String(payload.error);
        content.appendChild(note);
      } else if (payload.duration_ms !== undefined) {
        const note = document.createElement("small");
        const sources = (payload.summary?.sources || []).map((source) => {
          return { market: "行情", news: "新闻", annual_report: "年报", web: "网络公告" }[source] || source;
        });
        note.textContent = `完成${sources.length ? ` · ${sources.join("、")}` : ""} · ${payload.duration_ms}ms`;
        content.appendChild(note);
      }
      item.appendChild(content);
      list.appendChild(item);
    });
  } else {
    const item = document.createElement("li");
    item.className = "done";
    const content = document.createElement("div");
    const label = document.createElement("strong");
    label.textContent = "模型完成了本轮分析";
    content.appendChild(label);
    item.appendChild(content);
    list.appendChild(item);
  }
  if (budget) {
    const item = document.createElement("li");
    item.className = "done";
    const content = document.createElement("div");
    content.textContent = "检索结束后进入综合回答";
    item.appendChild(content);
    list.appendChild(item);
  }
  details.append(summary, list);
  article.appendChild(details);
  return article;
}

function renderChatHistory(history, events = []) {
  messages.innerHTML = "";
  if (!history.length) {
    messages.innerHTML = '<div class="empty-state">这个对话还没有消息，直接提问即可。</div>';
    return;
  }
  const eventsByTurn = new Map();
  events.forEach((event) => {
    if (!event.turn_id) return;
    const items = eventsByTurn.get(event.turn_id) || [];
    items.push(event);
    eventsByTurn.set(event.turn_id, items);
  });
  const turnsWithProgress = new Set();
  history.forEach((message) => {
    if (message.kind === "text" && (message.role === "user" || message.role === "assistant")) {
      addMessage(message.role, message.content);
      const bubble = messages.lastElementChild?.querySelector(".bubble");
      if (message.role === "user") {
        const progress = buildHistoricalProgress(eventsByTurn.get(message.turn_id));
        if (progress) messages.appendChild(progress);
        if (message.turn_id) turnsWithProgress.add(message.turn_id);
      }
      appendChatCitations(bubble, message.citations || []);
    } else if (message.kind === "system") {
      addMessage("error", message.content);
      if (message.turn_id) turnsWithProgress.add(message.turn_id);
    }
  });
  eventsByTurn.forEach((turnEvents, turnId) => {
    if (!turnsWithProgress.has(turnId)) {
      const progress = buildHistoricalProgress(turnEvents);
      if (progress) messages.appendChild(progress);
    }
  });
  messages.scrollTop = messages.scrollHeight;
}

async function selectChatThread(threadId) {
  if (state.chatStreaming && threadId !== state.chatThreadId) {
    addMessage("error", "当前回答仍在运行，完成后再切换对话。");
    return;
  }
  state.chatThreadId = threadId;
  setSessionValue(CHAT_THREAD_STORAGE, threadId);
  renderChatThreadTabs();
  messages.innerHTML = '<div class="empty-state">正在读取聊天记录…</div>';
  try {
    const [history, events] = await Promise.all([
      apiJson(`/api/agent/threads/${encodeURIComponent(threadId)}/messages?limit=200`),
      apiJson(`/api/agent/threads/${encodeURIComponent(threadId)}/events`),
    ]);
    renderChatHistory(history, events);
  } catch (error) {
    messages.innerHTML = `<div class="empty-state">${escapeHtml(error.message || "聊天记录读取失败")}</div>`;
  }
}

async function loadChatThreads(preferredThreadId = null) {
  try {
    const threads = (await apiJson("/api/agent/threads")).filter((thread) => thread.kind === "user");
    state.chatThreads = threads;
    if (!state.chatThreads.length) {
      const created = await apiJson("/api/agent/threads", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ title: "新对话" }),
      });
      state.chatThreads = [created];
    }
    const remembered = sessionValue(CHAT_THREAD_STORAGE);
    const resolved = [preferredThreadId, state.chatThreadId, remembered]
      .find((candidate) => candidate && state.chatThreads.some((thread) => thread.thread_id === candidate))
      || state.chatThreads[0].thread_id;
    renderChatThreadTabs();
    await selectChatThread(resolved);
  } catch (error) {
    messages.innerHTML = `<div class="empty-state">${escapeHtml(error.message || "聊天线程加载失败")}</div>`;
  }
}

async function createChatThread() {
  if (state.chatStreaming) {
    addMessage("error", "当前回答仍在运行，完成后再新建对话。");
    return;
  }
  try {
    const thread = await apiJson("/api/agent/threads", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title: "新对话" }),
    });
    state.chatThreads.unshift(thread);
    await selectChatThread(thread.thread_id);
    questionInput.focus();
  } catch (error) {
    addMessage("error", error.message || "新建对话失败");
  }
}

async function renameChatThread(threadId) {
  const thread = state.chatThreads.find((item) => item.thread_id === threadId);
  if (!thread) return;
  const title = window.prompt("对话名称", thread.title);
  if (!title?.trim()) return;
  try {
    const updated = await apiJson(`/api/agent/threads/${encodeURIComponent(threadId)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title: title.trim().slice(0, 80) }),
    });
    Object.assign(thread, updated);
    renderChatThreadTabs();
  } catch (error) {
    addMessage("error", error.message || "重命名失败");
  }
}

async function archiveChatThread(threadId) {
  if (state.chatStreaming) {
    addMessage("error", "当前回答仍在运行，完成后再归档对话。");
    return;
  }
  try {
    await apiJson(`/api/agent/threads/${encodeURIComponent(threadId)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ status: "archived" }),
    });
    state.chatThreads = state.chatThreads.filter((thread) => thread.thread_id !== threadId);
    if (!state.chatThreads.length) {
      await loadChatThreads();
    } else {
      const nextId = state.chatThreadId === threadId ? state.chatThreads[0].thread_id : state.chatThreadId;
      await loadChatThreads(nextId);
    }
  } catch (error) {
    addMessage("error", error.message || "归档对话失败");
  }
}

async function maybeRenameActiveChatThread(question) {
  const thread = activeChatThread();
  if (!thread || !["新对话", "新研究"].includes(thread.title)) return;
  try {
    const updated = await apiJson(`/api/agent/threads/${encodeURIComponent(thread.thread_id)}`, {
      method: "PATCH",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ title: titleFromQuestion(question) }),
    });
    Object.assign(thread, updated);
    renderChatThreadTabs();
  } catch {
    /* The conversation is already persisted; renaming is cosmetic. */
  }
}

function formatChatElapsed() {
  if (!state.chatStartedAt) return "0.0s";
  const seconds = (performance.now() - state.chatStartedAt) / 1000;
  return seconds < 10 ? `${seconds.toFixed(1)}s` : `${Math.floor(seconds)}s`;
}

function stopChatProgressTimer() {
  if (state.chatProgressTimer) {
    window.clearInterval(state.chatProgressTimer);
    state.chatProgressTimer = null;
  }
}

function startChatProgressTimer() {
  stopChatProgressTimer();
  state.chatStartedAt = performance.now();
  if (chatElapsed) chatElapsed.textContent = "0.0s";
  chatStatus.textContent = "运行中 · 0.0s";
  state.chatProgressTimer = window.setInterval(() => {
    const elapsed = formatChatElapsed();
    if (chatElapsed) chatElapsed.textContent = elapsed;
    chatStatus.textContent = `运行中 · ${elapsed}`;
  }, 200);
}

function resetChatProgress() {
  stopChatProgressTimer();
  const article = document.createElement("article");
  article.className = "message progress";
  const details = document.createElement("details");
  details.className = "chat-progress";
  details.open = true;
  const summary = document.createElement("summary");
  summary.className = "chat-progress-heading";
  const title = document.createElement("strong");
  const meta = document.createElement("span");
  const spinner = document.createElement("i");
  spinner.className = "chat-progress-spinner";
  const elapsed = document.createElement("time");
  elapsed.textContent = "0.0s";
  meta.append(spinner, elapsed);
  summary.append(title, meta);
  const list = document.createElement("ol");
  details.append(summary, list);
  article.appendChild(details);
  messages.appendChild(article);
  messages.scrollTop = messages.scrollHeight;
  chatProgress = details;
  chatProgressTitle = title;
  chatProgressList = list;
  chatElapsed = elapsed;
  chatProgressTitle.textContent = "正在建立研究任务";
  startChatProgressTimer();
}

function setChatProgressTitle(text) {
  if (chatProgressTitle) chatProgressTitle.textContent = text;
}

function appendChatProgress(text, detail = "", kind = "running") {
  if (!chatProgressList) return null;
  const item = document.createElement("li");
  item.className = kind;
  const content = document.createElement("div");
  const label = document.createElement("strong");
  label.textContent = text;
  content.appendChild(label);
  if (detail) {
    const note = document.createElement("small");
    note.textContent = detail;
    content.appendChild(note);
  }
  const time = document.createElement("time");
  time.textContent = formatChatElapsed();
  item.append(content, time);
  chatProgressList.appendChild(item);
  chatProgressList.scrollTop = chatProgressList.scrollHeight;
  return item;
}

function collapseChatProgress() {
  if (chatProgress) {
    chatProgress.classList.add("completed");
    chatProgress.open = false;
  }
}

function finishChatProgressItem(item, detail = null, kind = "done") {
  if (!item) return;
  item.className = kind;
  const time = item.querySelector("time");
  if (time) time.textContent = formatChatElapsed();
  if (detail) {
    let note = item.querySelector("small");
    if (!note) {
      note = document.createElement("small");
      item.querySelector("div")?.appendChild(note);
    }
    note.textContent = detail;
  }
}

function describeChatTool(payload) {
  const args = payload.args || {};
  const sources = (args.sources || payload.summary?.sources || []).filter(Boolean);
  const sourceLabels = { market: "行情", news: "新闻", annual_report: "年报", web: "网络公告" };
  if (payload.tool_name === "research_search") {
    const names = sources.map((source) => sourceLabels[source] || source).join("、");
    return {
    label: names ? `检索${names}资料` : "检索相关资料",
      detail: args.query ? String(args.query) : "",
    };
  }
  if (payload.tool_name === "resolve_security") {
    return { label: "解析证券代码", detail: args.query ? String(args.query) : "" };
  }
  if (payload.tool_name === "list_watchlist") {
    return { label: "读取监护清单", detail: "" };
  }
  return {
    label: `执行工具 ${payload.tool_name || "未知"}`,
    detail: "",
  };
}

function appendChatCitations(bubble, citations) {
  if (!bubble || !citations?.length || bubble.querySelector(".chat-citations")) return;
  const details = document.createElement("details");
  details.className = "chat-citations";
  const summary = document.createElement("summary");
  summary.textContent = `来源与引用（${citations.length}）`;
  const list = document.createElement("ol");
  citations.forEach((citation) => {
    const item = document.createElement("li");
    item.textContent = `[${citation.citation_id}] ${citation.title || citation.source_type || "证据"}`;
    if (citation.locator) item.textContent += ` · ${citation.locator}`;
    list.appendChild(item);
  });
  details.append(summary, list);
  bubble.appendChild(details);
}

async function sendQuestion(question) {
  if (!state.chatThreadId) await loadChatThreads();
  state.chatStreaming = true;
  resetChatProgress();
  chatStatus.classList.add("busy");
  sendButton.disabled = true;
  let answerBubble = null;
  let answerText = "";
  let citations = [];
  let streamError = null;
  let terminalEvent = null;
  let generatingItem = null;
  const toolItems = new Map();
  try {
    const headers = llmHeaders({ "Content-Type": "application/json", "Accept": "text/event-stream" });
    const requestStream = () => fetch("/api/chat/stream", {
      method: "POST",
      headers,
      body: JSON.stringify({
        question,
        thread_id: state.chatThreadId,
        mode: "standard",
      }),
    });
    let response = await requestStream();
    if (response.status === 409 && state.chatThreadId) {
      state.chatThreadId = null;
      response = await requestStream();
    }
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.detail || `请求失败（${response.status}）`);
    }
    const connectedItem = appendChatProgress("研究任务已建立", "开始规划需要调用的数据和工具");
    finishChatProgressItem(connectedItem);
    setChatProgressTitle("模型正在规划研究步骤");
    maybeRenameActiveChatThread(question);

    await parseSSEStream(response, (event) => {
      const type = event.event_type;
      const payload = event.payload || {};
      if (event.thread_id) state.chatThreadId = event.thread_id;
      if (type === "turn.started") {
        appendChatProgress(
          "开始分析",
          "正在判断任务类型并规划所需资料",
        ).className = "done";
      } else if (type === "step.completed") {
        if (payload.status === "budget_exhausted") {
          const item = appendChatProgress("检索已结束", "开始整理已有证据并生成结论", "done");
          item.querySelector("time").textContent = formatChatElapsed();
          setChatProgressTitle("正在整理已有证据");
        } else {
          const calls = payload.tool_calls || [];
          appendChatProgress(
            calls.length ? "模型已规划下一步" : "模型已完成分析规划",
            calls.length ? `准备执行 ${calls.join("、")}` : "进入综合回答",
            "done",
          );
        }
      } else if (type === "tool.started") {
        const description = describeChatTool(payload);
        const item = appendChatProgress(description.label, description.detail);
        toolItems.set(payload.call_id, item);
        setChatProgressTitle(description.label);
      } else if (type === "tool.completed") {
        const item = toolItems.get(payload.call_id);
        if (payload.error) {
          finishChatProgressItem(item, `失败：${payload.error}`, "error");
        } else {
          const sources = (payload.summary?.sources || []).map((source) => {
            return { market: "行情", news: "新闻", annual_report: "年报", web: "网络公告" }[source] || source;
          });
          finishChatProgressItem(
            item,
            `完成${sources.length ? ` · ${sources.join("、")}` : ""} · ${payload.duration_ms || 0}ms`,
          );
        }
        setChatProgressTitle("模型正在分析检索结果");
      } else if (type === "approval.required") {
        appendChatProgress("等待操作确认", payload.summary || payload.tool_name || "", "running");
        setChatProgressTitle("等待确认");
      } else if (type === "message.delta") {
        if (!answerBubble) {
          const article = document.createElement("article");
          article.className = "message assistant";
          answerBubble = document.createElement("div");
          answerBubble.className = "bubble";
          article.appendChild(answerBubble);
          messages.appendChild(article);
          generatingItem = appendChatProgress("生成研究结论", "基于已取得的证据组织回答");
        }
        setChatProgressTitle("正在生成回答");
        answerText += payload.text || "";
        answerBubble.textContent = answerText;
        messages.scrollTop = messages.scrollHeight;
      } else if (type === "message.completed") {
        citations = payload.citations || [];
        finishChatProgressItem(generatingItem, "回答已生成");
        appendChatCitations(answerBubble, citations);
      } else if (type === "turn.completed") {
        terminalEvent = "completed";
        finishChatProgressItem(generatingItem, "完成");
        setChatProgressTitle("分析完成");
      } else if (type === "turn.failed") {
        terminalEvent = "failed";
        streamError = payload.error || "Agent 运行失败";
        appendChatProgress("运行失败", streamError, "error");
        setChatProgressTitle("分析失败");
      } else if (type === "turn.cancelled") {
        terminalEvent = "cancelled";
        streamError = "本轮分析已停止";
        appendChatProgress("分析已停止", "", "error");
        setChatProgressTitle("分析已停止");
      }
    });

    if (streamError) throw new Error(streamError);
    if (!terminalEvent) throw new Error("进度连接提前结束，请重试。");
    if (!answerText && state.chatThreadId) {
      const items = await apiJson(`/api/agent/threads/${encodeURIComponent(state.chatThreadId)}/messages?limit=20`);
      const assistant = [...items].reverse().find((item) => item.role === "assistant" && item.kind === "text");
      if (assistant) {
        answerText = assistant.content;
        citations = assistant.citations || [];
        if (!answerBubble) {
          addMessage("assistant", answerText);
          answerBubble = messages.querySelector(".message.assistant:last-of-type .bubble");
        }
        appendChatCitations(answerBubble, citations);
      }
    }
    if (!answerText) addMessage("assistant", "Agent 未生成回答。");
    const totalTime = formatChatElapsed();
    stopChatProgressTimer();
    if (chatElapsed) chatElapsed.textContent = totalTime;
    chatStatus.textContent = `完成 · ${totalTime}`;
    collapseChatProgress();
    if (!state.chatThreads.some((thread) => thread.thread_id === state.chatThreadId)) {
      await loadChatThreads(state.chatThreadId);
    }
  } catch (error) {
    stopChatProgressTimer();
    chatStatus.textContent = "失败";
    appendChatProgress("请求失败", error.message || "聊天请求失败", "error");
    addMessage("error", error.message || "聊天请求失败。");
    collapseChatProgress();
  } finally {
    state.chatStreaming = false;
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
  if (!items.length) { newsList.innerHTML = '<div class="empty-state">当前暂无可展示的新闻。</div>'; return; }
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
  const extractionStatusLabel = {
    pending_parse: "PDF可用 · 待解析",
    needs_review: "PDF可用 · 需复核",
    ok: "文本可用",
  };
  reportList.innerHTML = items.map((report) => `
    <article class="report-card" data-report-id="${escapeHtml(report.report_id)}">
      <h3>${escapeHtml(report.title)}</h3>
      <div class="report-meta"><span>${escapeHtml(report.stock_code)} · ${escapeHtml(report.company_name || "公司名称未知")}</span><span>${escapeHtml(String(report.report_year))} 年报</span><span>${escapeHtml(formatDateTime(report.announcement_time))}</span><span>${escapeHtml(extractionStatusLabel[report.extraction_status] || report.extraction_status || "未知状态")}</span></div>
      <div class="report-actions"><a href="/api/reports/${encodeURIComponent(report.report_id)}/download" target="_blank" rel="noreferrer">打开 PDF</a>${report.page_count > 0 ? '<button type="button" data-action="read-page">查看首页文本</button>' : '<span class="muted-text">文本提取完成后可查看</span>'}</div>
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
    if (pageId === "agentPage" && !state.agentThreadId) loadAgentThreads();
    requestAnimationFrame(() => state.chart?.resize());
  });
});

newAgentThreadButton?.addEventListener("click", async () => {
  try {
    const thread = await apiJson("/api/agent/threads", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({}) });
    state.agentThreadId = thread.thread_id;
    await loadAgentThreads(thread.thread_id);
    agentQuestion.focus();
  } catch (error) {
    addAgentSystemMessage(error.message);
  }
});

agentThreadList?.addEventListener("click", async (event) => {
  const row = event.target.closest("[data-thread-id]");
  if (!row) return;
  const threadId = row.dataset.threadId;
  const action = event.target.closest("button")?.dataset.action;
  if (action === "archive") {
    try {
      await apiJson(`/api/agent/threads/${encodeURIComponent(threadId)}`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ status: "archived" }) });
      if (state.agentThreadId === threadId) state.agentThreadId = null;
      await loadAgentThreads();
    } catch (error) { addAgentSystemMessage(error.message); }
    return;
  }
  await selectAgentThread(threadId);
});

agentComposer?.addEventListener("submit", async (event) => {
  event.preventDefault();
  const question = agentQuestion.value.trim();
  if (!question || state.agentStreaming) return;
  agentQuestion.value = "";
  agentToolTimeline.innerHTML = "";
  try {
    await submitAgentTurn(question);
  } catch (error) {
    addAgentSystemMessage(error.message || "研究请求失败");
    state.agentStreaming = false;
    agentSendButton.disabled = false;
  }
});

modelSettingsForm?.addEventListener("submit", (event) => {
  event.preventDefault();
  const config = getLlmConfig();
  if (!config.apiKey) {
    renderLlmSettingsStatus();
    llmApiKeyInput.focus();
    return;
  }
  setSessionValue(LLM_KEY_STORAGE, config.apiKey);
  setSessionValue(LLM_MODEL_STORAGE, config.model);
  renderLlmSettingsStatus();
});

llmApiKeyInput?.addEventListener("input", renderLlmSettingsStatus);
llmModelSelect?.addEventListener("change", () => {
  setSessionValue(LLM_MODEL_STORAGE, llmModelSelect.value);
  renderLlmSettingsStatus();
});
clearLlmKeyButton?.addEventListener("click", () => {
  llmApiKeyInput.value = "";
  setSessionValue(LLM_KEY_STORAGE, "");
  renderLlmSettingsStatus();
  llmApiKeyInput.focus();
});

agentQuestion?.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    agentComposer.requestSubmit();
  }
});

cancelAgentTurnButton?.addEventListener("click", async () => {
  if (!state.agentThreadId || !state.agentTurnId) return;
  try {
    await apiJson(`/api/agent/threads/${encodeURIComponent(state.agentThreadId)}/turns/${encodeURIComponent(state.agentTurnId)}/cancel`, { method: "POST" });
    agentRunStatus.textContent = "正在停止";
  } catch (error) { addAgentSystemMessage(error.message); }
});

refreshInspectorButton?.addEventListener("click", () => loadInspector());
manageWebDomainsButton?.addEventListener("click", async () => {
  try {
    const domains = await apiJson("/api/agent/web-domains");
    const current = domains.map((item) => item.host_pattern).join("\n");
    const value = window.prompt("已允许域名。输入域名新增；输入 -域名 删除；留空取消。\n\n" + current, "");
    if (!value) return;
    const trimmed = value.trim();
    if (trimmed.startsWith("-")) {
      const host = trimmed.slice(1).trim();
      await apiJson(`/api/agent/web-domains/${encodeURIComponent(host)}`, { method: "DELETE" });
      addAgentSystemMessage(`已从网页白名单移除：${host}`);
    } else {
      await apiJson("/api/agent/web-domains", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ host_pattern: trimmed }) });
      addAgentSystemMessage(`已加入网页白名单：${trimmed}`);
    }
  } catch (error) { addAgentSystemMessage(error.message); }
});

agentMessages?.addEventListener("click", async (event) => {
  if (event.target.closest('[data-action="open-monitor-dashboard"]')) {
    document.querySelector('[data-page="monitorPage"]')?.click();
    return;
  }
  const button = event.target.closest("button[data-decision]");
  const card = button?.closest("[data-approval-id]");
  if (button && card) {
    await submitApproval(card.dataset.approvalId, button.dataset.decision === "approve", card);
  }
});

newChatThreadButton?.addEventListener("click", createChatThread);
chatThreadTabs?.addEventListener("click", async (event) => {
  const archiveButton = event.target.closest("[data-archive-chat]");
  if (archiveButton) {
    event.stopPropagation();
    await archiveChatThread(archiveButton.dataset.archiveChat);
    return;
  }
  const tab = event.target.closest("[data-chat-thread]");
  if (tab) await selectChatThread(tab.dataset.chatThread);
});
chatThreadTabs?.addEventListener("dblclick", async (event) => {
  const tab = event.target.closest("[data-chat-thread]");
  if (tab) await renameChatThread(tab.dataset.chatThread);
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
chatForm.addEventListener("submit", (event) => { event.preventDefault(); const question = questionInput.value.trim(); if (!question || state.chatStreaming) return; addMessage("user", question); questionInput.value = ""; sendQuestion(question); });
questionInput.addEventListener("keydown", (event) => { if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) chatForm.requestSubmit(); });
refreshNewsButton.addEventListener("click", loadNews);
pageSizeSelect.addEventListener("change", () => { state.newsPage = 1; state.newsPageSize = Number(pageSizeSelect.value); loadNews(); });
prevPageButton.addEventListener("click", () => { if (state.newsPage > 1) { state.newsPage -= 1; loadNews(); } });
nextPageButton.addEventListener("click", () => { state.newsPage += 1; loadNews(); });
window.addEventListener("resize", () => state.chart?.resize());

initializeDateRange();
loadLlmSettings();
loadChatThreads();
loadSecurityOptions();
loadNews();
for (let year = new Date().getFullYear(); year >= new Date().getFullYear() - 10; year -= 1) {
  const option = document.createElement("option");
  option.value = String(year);
  option.textContent = `${year} 年`;
  reportYear.append(option);
}
