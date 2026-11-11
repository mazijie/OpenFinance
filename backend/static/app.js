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
const securitySearch = document.querySelector("#securitySearch");
const securityResults = document.querySelector("#securityResults");
const MARKET_ADJUSTMENT = "none";
const marketMessage = document.querySelector("#marketMessage");
const stockLastPrice = document.querySelector("#stockLastPrice");
const stockChangePercent = document.querySelector("#stockChangePercent");
const stockPreviousClose = document.querySelector("#stockPreviousClose");
const stockPriceTime = document.querySelector("#stockPriceTime");
const stockVolume = document.querySelector("#stockVolume");
const stockMarketCap = document.querySelector("#stockMarketCap");
const stockPe = document.querySelector("#stockPe");
const marketChart = document.querySelector("#marketChart");
const chartEmpty = document.querySelector("#chartEmpty");
const chartSubtitle = document.querySelector("#chartSubtitle");
const marketSourceBadge = document.querySelector("#marketSourceBadge");
const historyStatus = document.querySelector("#historyStatus");
const loadOlderButton = document.querySelector("#loadOlderButton");

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
  marketPeriod: "daily",
  marketSummary: null,
  intradayRequestId: 0,
  historyBars: [],
  historyBefore: null,
  historyHasMore: false,
  historyLoading: false,
  historyFailed: false,
  historyChecking: false,
  historyCheckFailed: false,
  historyCheckExpired: false,
  historyAvailability: null,
  historySymbol: null,
  liveQuote: null,
  lastProvisionalDate: null,
  lastReconcileAt: 0,
  reconciling: false,
  historyReady: false,
  securities: [],
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
    state.securities = items;
    securitySearch.disabled = false;
    securitySearch.placeholder = items.length ? "输入股票名称或代码" : "暂无可用证券";
    symbolInput.innerHTML = items.length
      ? items.map((item) => `<option value="${escapeHtml(item.symbol)}" data-exchange="${escapeHtml(item.exchange)}">${escapeHtml(item.symbol)} · ${escapeHtml(item.name)} · ${escapeHtml(item.exchange)}</option>`).join("")
      : '<option value="" selected disabled>暂无可用证券</option>';
    if (items.some((item) => item.symbol === "600519.SH")) symbolInput.value = "600519.SH";
    if (items.length) securitySearch.value = `${items[symbolInput.selectedIndex].name} · ${items[symbolInput.selectedIndex].symbol}`;
    if (items.length && marketForm.checkValidity()) loadMarket();
    if (items.length) loadRealtimeQuotes();
  } catch (error) {
    console.warn(error.message || "证券列表加载失败");
    symbolInput.innerHTML = '<option value="" selected disabled>证券列表加载失败</option>';
    securitySearch.placeholder = "证券列表加载失败";
  }
}

function pad(value) {
  return String(value).padStart(2, "0");
}

function toDateInputValue(date) {
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
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
    throw new Error("请先在模型设置中填写 DeepSeek API Key。");
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
  modelSettingsStatus.textContent = ready ? `已配置 · ${config.model}` : "未设置 API Key";
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
  const approval = message.kind === "approval" ? message.payload?.approval : null;
  const approvalActions = !approval?.status || approval.status === "pending" ? '<div class="approval-actions"><button type="button" class="primary-button" data-decision="approve">批准</button><button type="button" class="secondary-button" data-decision="reject">拒绝</button></div>' : "";
  const approvalHtml = approval ? `<div class="approval-card" data-approval-id="${escapeHtml(approval.approval_id)}"><h4>需要确认：${escapeHtml(approval.tool_name)}</h4><p>${escapeHtml(approval.summary || JSON.stringify(approval.args || {}))}</p>${approvalActions}</div>` : "";
  const citationHtml = citations.length ? `<details class="agent-citations"><summary>来源与引用（${citations.length}）</summary><ol>${citations.map((citation) => `<li>[${citation.citation_id}] ${citation.url ? `<a href="${escapeHtml(citation.url)}" target="_blank" rel="noreferrer">${escapeHtml(citation.title)}</a>` : escapeHtml(citation.title)}${citation.content_type === "summary" ? " · 摘要" : ""}${citation.locator ? ` · ${escapeHtml(citation.locator)}` : ""}</li>`).join("")}</ol></details>` : "";
  return `<article class="agent-message ${escapeHtml(message.role)} ${escapeHtml(message.kind)}"><div class="agent-bubble">${escapeHtml(message.content)}${approvalHtml}${citationHtml}</div></article>`;
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
  marketMessage.hidden = !message;
}

function setLoading(isLoading) {
  if (isLoading) {
    marketSourceBadge.textContent = "正在查询";
    setMarketMessage("正在获取最新行情数据…", "info");
  }
}

function renderChart(summary, viewport = null) {
  const bars = [...(summary.bars || [])].sort((a, b) => String(a.trading_date).localeCompare(String(b.trading_date)));
  const hasData = bars.length > 0;
  chartEmpty.hidden = hasData;
  chartEmpty.textContent = "暂无行情数据";
  marketChart.classList.toggle("is-empty", !hasData);
  chartSubtitle.textContent = hasData ? `${formatDate(bars[0].period_first_date || bars[0].trading_date)} — ${formatDate(bars[bars.length - 1].trading_date)} · ${bars.length} ${state.marketPeriod === "daily" ? "个交易日" : "个周期"}` : "暂无数据";
  if (!hasData || typeof window.echarts === "undefined") {
    if (hasData && typeof window.echarts === "undefined") chartEmpty.textContent = "图表组件加载失败";
    if (state.chart) state.chart.clear();
    return;
  }
  ensureMarketChart();
  const dates = bars.map((bar) => bar.trading_date);
  const candles = bars.map((bar) => [Number(bar.open), Number(bar.close), Number(bar.low), Number(bar.high)]);
  const volumes = bars.map((bar) => ({ value: bar.volume == null ? null : Number(bar.volume), itemStyle: { color: Number(bar.close) >= Number(bar.open) ? "#ef6a6a" : "#4aa38a" } }));
  state.chart.setOption({
    animation: false,
    tooltip: { trigger: "axis", axisPointer: { type: "cross" }, backgroundColor: "rgba(20, 31, 49, .94)", borderWidth: 0, textStyle: { color: "#fff" }, formatter: (params) => {
      const index = params[0]?.dataIndex ?? 0;
      const bar = bars[index];
      const flags = [bar.is_suspended ? "停牌" : "", bar.is_st ? "ST" : ""].filter(Boolean).join(" / ");
      return `<strong>${escapeHtml(formatDate(bar.trading_date))}</strong><br/>开盘 ${formatNumber(bar.open)}　${bar.provisional ? "最新" : "收盘"} ${formatNumber(bar.close)}<br/>最高 ${formatNumber(bar.high)}　最低 ${formatNumber(bar.low)}<br/>成交量 ${bar.volume == null ? "暂无" : formatNumber(bar.volume, 0)}${flags ? `<br/>${escapeHtml(flags)}` : ""}`;
    } },
    axisPointer: { link: [{ xAxisIndex: "all" }] },
    grid: [{ left: 56, right: 22, top: 24, height: "58%" }, { left: 56, right: 22, top: "72%", height: "16%" }],
    xAxis: [{ type: "category", data: dates, boundaryGap: true, axisLine: { lineStyle: { color: "#d7dee8" } }, axisLabel: { color: "#768397", formatter: (value) => String(value).slice(5) } }, { type: "category", gridIndex: 1, data: dates, axisLabel: { show: false }, axisLine: { show: false } }],
    yAxis: [{ scale: true, splitLine: { lineStyle: { color: "#eef2f6" } }, axisLabel: { color: "#768397", hideOverlap: true } }, { gridIndex: 1, min: 0, splitNumber: 2, splitLine: { show: false }, axisLabel: { color: "#768397", hideOverlap: true, formatter: (value) => value >= 100000000 ? `${(value / 100000000).toFixed(1)}亿` : value >= 10000 ? `${(value / 10000).toFixed(1)}万` : String(value) } }],
    dataZoom: [{ type: "inside", xAxisIndex: [0, 1], ...(viewport || { start: bars.length > 90 ? 30 : 0, end: 100 }) }, { type: "slider", xAxisIndex: [0, 1], bottom: 4, height: 18, borderColor: "#d7dee8", fillerColor: "rgba(15, 118, 110, .14)", textStyle: { color: "#768397" } }],
    series: [{ name: "价格", type: "candlestick", data: candles, itemStyle: { color: "#ef6a6a", color0: "#4aa38a", borderColor: "#ef6a6a", borderColor0: "#4aa38a" } }, { name: "成交量", type: "bar", xAxisIndex: 1, yAxisIndex: 1, data: volumes }],
  }, true);
}

function ensureMarketChart() {
  if (state.chart) return;
  state.chart = window.echarts.init(marketChart);
  state.chart.on("datazoom", () => {
    if (state.marketPeriod === "intraday" || state.historyLoading || state.historyFailed || !state.historyHasMore) return;
    const zoom = state.chart.getOption().dataZoom?.[0];
    if (zoom && zoom.start <= 5) loadOlderHistory();
  });
}

function aggregateBars(bars, period) {
  if (period === "daily") return bars;
  const groups = new Map();
  for (const bar of [...bars].sort((a, b) => String(a.trading_date).localeCompare(String(b.trading_date)))) {
    const date = new Date(`${String(bar.trading_date).slice(0, 10)}T00:00:00Z`);
    if (period === "weekly") date.setUTCDate(date.getUTCDate() - (date.getUTCDay() + 6) % 7);
    else date.setUTCDate(1);
    const key = date.toISOString().slice(0, 10);
    const group = groups.get(key);
    if (!group) groups.set(key, { ...bar, period_first_date: bar.trading_date });
    else {
      group.trading_date = bar.trading_date;
      group.high = Math.max(Number(group.high), Number(bar.high));
      group.low = Math.min(Number(group.low), Number(bar.low));
      group.close = bar.close;
      group.volume = group.volume == null || bar.volume == null ? null : Number(group.volume) + Number(bar.volume);
      group.is_suspended ||= bar.is_suspended;
      group.is_st ||= bar.is_st;
      group.provisional ||= bar.provisional;
    }
  }
  return [...groups.values()];
}

function chinaDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-CA", { timeZone: "Asia/Shanghai", year: "numeric", month: "2-digit", day: "2-digit" }).formatToParts(date);
  const get = (name) => parts.find((part) => part.type === name)?.value;
  return `${get("year")}-${get("month")}-${get("day")}`;
}

function provisionalDailyBar(quote, bars, symbol, now) {
  if (!quote || quote.symbol !== symbol || !["fresh", "closed"].includes(quote.status) || quote.quote_status !== "valid") return null;
  const timestamp = new Date(quote.source_time).getTime();
  const capture = new Date(quote.captured_at).getTime();
  if (!Number.isFinite(timestamp) || !Number.isFinite(capture) || timestamp > now || capture > now) return null;
  const day = chinaDate(timestamp);
  if (day !== chinaDate(now) || chinaDate(capture) !== day || bars.some((bar) => bar.trading_date >= day)) return null;
  const values = [quote.day_open, quote.day_high, quote.day_low, quote.price];
  if (values.some((value) => value == null || !Number.isFinite(Number(value)) || Number(value) <= 0)) return null;
  const [open, high, low, close] = values.map(Number);
  if (high < Math.max(open, close) || low > Math.min(open, close) || high < low) return null;
  const volume = quote.volume_lots == null || !Number.isFinite(Number(quote.volume_lots)) || Number(quote.volume_lots) < 0 ? null : Number(quote.volume_lots) * 100;
  return { trading_date: day, open, high, low, close, volume, provisional: true };
}

function selectedQuoteSymbol() {
  const selected = symbolInput.value.trim();
  const exchange = symbolInput.selectedOptions[0]?.dataset.exchange?.toUpperCase();
  return /^\d{6}$/.test(selected) && ["SH", "SZ", "BJ"].includes(exchange) ? `${selected}.${exchange}` : selected;
}

function displayDailyBars() {
  const bars = state.historyBars;
  const provisional = provisionalDailyBar(state.liveQuote, bars, selectedQuoteSymbol(), Date.now());
  if (provisional) state.lastProvisionalDate = provisional.trading_date;
  return provisional ? [...bars, provisional] : bars;
}

function currentChartViewport(bars) {
  const zoom = state.chart?.getOption().dataZoom?.[0];
  if (!zoom || !bars.length) return null;
  return { startValue: bars[Math.round((bars.length - 1) * zoom.start / 100)].trading_date,
    endValue: bars[Math.round((bars.length - 1) * zoom.end / 100)].trading_date };
}

function renderHistoryChart(viewport = null) {
  const daily = displayDailyBars();
  const bars = aggregateBars(daily, state.marketPeriod);
  renderChart({ bars }, viewport);
  if (daily.some((bar) => bar.provisional)) {
    const previous = state.historyBars.at(-1)?.trading_date;
    const last = daily.at(-1).trading_date;
    if (!previous) chartSubtitle.textContent += " · 暂无历史行情";
    else if ((Date.parse(`${last}T00:00:00Z`) - Date.parse(`${previous}T00:00:00Z`)) / 86400000 > 1) chartSubtitle.textContent += " · 历史数据可能有缺口";
  }
}

function renderSelectedPeriod() {
  const period = state.marketPeriod;
  const intraday = period === "intraday";
  historyStatus.hidden = intraday;
  loadOlderButton.hidden = intraday || (!state.historyHasMore && !state.historyCheckFailed);
  if (intraday) { loadIntraday(); return; }
  ++state.intradayRequestId;
  renderHistoryChart();
}

async function loadIntraday() {
  const requestId = ++state.intradayRequestId;
  const selectedSymbol = symbolInput.value;
  const exchange = symbolInput.selectedOptions[0]?.dataset.exchange?.toUpperCase();
  const symbol = /^\d{6}$/.test(selectedSymbol) && ["SH", "SZ", "BJ"].includes(exchange) ? `${selectedSymbol}.${exchange}` : selectedSymbol;
  chartEmpty.hidden = false;
  chartEmpty.textContent = "正在加载分时数据…";
  chartSubtitle.textContent = "分时走势";
  if (state.chart) state.chart.clear();
  if (!symbol) { chartEmpty.textContent = "请选择证券"; return; }
  try {
    const response = await fetch(`/api/realtime-quotes/intraday?${new URLSearchParams({ symbol })}`);
    if (!response.ok) throw new Error("分时数据暂不可用");
    const data = await response.json();
    if (requestId !== state.intradayRequestId || state.marketPeriod !== "intraday" || selectedSymbol !== symbolInput.value) return;
    const points = data.points || [];
    if (!points.length) { chartEmpty.textContent = "暂无分时数据"; return; }
    chartEmpty.hidden = true;
    marketChart.classList.remove("is-empty");
    if (!state.chart && window.echarts) ensureMarketChart();
    if (!state.chart) { chartEmpty.hidden = false; chartEmpty.textContent = "图表组件加载失败"; return; }
    const series = intradaySeries(points);
    if (!series.length) { chartEmpty.hidden = false; chartEmpty.textContent = "暂无交易时段内的分时数据"; return; }
    const previousClose = intradayPreviousClose(state.liveQuote?.previous_close);
    const firstPoint = series.find((point) => point.time);
    chartSubtitle.textContent = `${formatRealtimeTime(firstPoint.time).split(' ')[0]} · 分时走势${points.some((point) => point.incomplete) ? " · 部分数据待更新" : ""}`;
    state.chart.setOption({ animation: false, tooltip: { trigger: "axis", formatter: (params) => {
      const item = params.find((entry) => entry.data?.time);
      if (!item) return "";
      return `${escapeHtml(formatRealtimeTime(item.data.time))}<br/>价格 ${formatNumber(item.data.value[1])}`;
    } }, grid: { left: 56, right: 22, top: 25, bottom: 35 }, xAxis: { type: "value", min: 0, max: 240, interval: 60, axisLabel: { formatter: (value) => ({ 0: "09:30", 60: "10:30", 120: "11:30 / 13:00", 180: "14:00", 240: "15:00" })[value] || "" } }, yAxis: { type: "value", scale: true, axisLabel: { hideOverlap: true }, splitLine: { lineStyle: { color: "#eef2f6" } } }, series: [{ name: "价格", type: "line", showSymbol: false, connectNulls: false, data: series, lineStyle: { color: "#0f766e" }, markLine: { symbol: "none", silent: true, data: previousClose == null ? [] : [{ name: "昨收", yAxis: previousClose, lineStyle: { color: "#9aa6b2", type: "dashed", width: 1 }, label: { show: true, formatter: "昨收", position: "insideEndTop", color: "#768397" } }] } }] }, true);
  } catch (error) {
    if (requestId === state.intradayRequestId && state.marketPeriod === "intraday") chartEmpty.textContent = "分时数据暂不可用";
  }
}

function intradayTradingMinute(value) {
  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-GB", { timeZone: "Asia/Shanghai", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" }).formatToParts(timestamp);
  const get = (type) => Number(parts.find((part) => part.type === type)?.value);
  const minute = get("hour") * 60 + get("minute") + get("second") / 60;
  if (minute < 570 || minute > 900 || (minute > 690 && minute < 780)) return null;
  return minute <= 690 ? minute - 570 : minute - 780 + 120;
}

function intradaySeries(points) {
  const result = [];
  for (const point of points) {
    const x = intradayTradingMinute(point.time);
    if (x == null || point.price == null) continue;
    const previous = result.at(-1);
    if (previous?.time && x - previous.value[0] > 1 + 1e-6) result.push({ value: [x, null], time: null });
    result.push({ value: [x, Number(point.price)], time: point.time });
  }
  return result;
}

function intradayPreviousClose(value) {
  const price = Number(value);
  return value != null && Number.isFinite(price) && price > 0 ? price : null;
}

function clearMarket() {
  state.marketSummary = null;
  if (state.marketPeriod !== "intraday") renderChart({ bars: [] });
}

async function loadMarket() {
  if (!marketForm.checkValidity()) return;
  const requestId = ++state.marketRequestId;
  if (state.marketAbortController) state.marketAbortController.abort();
  state.marketAbortController = new AbortController();
  state.historyBars = [];
  state.historyBefore = null;
  state.historyHasMore = false;
  state.historyLoading = false;
  state.historyFailed = false;
  state.historyChecking = false;
  state.historyCheckFailed = false;
  state.historyCheckExpired = false;
  state.historyAvailability = null;
  state.historyReady = false;
  state.lastProvisionalDate = null;
  state.lastReconcileAt = 0;
  const selected = symbolInput.value.trim();
  const exchange = symbolInput.selectedOptions[0]?.dataset.exchange?.toUpperCase();
  state.historySymbol = /^\d{6}$/.test(selected) && ["SH", "SZ", "BJ"].includes(exchange) ? `${selected}.${exchange}` : selected;
  ++state.intradayRequestId;
  clearMarket();
  if (state.marketPeriod === "intraday") { marketSourceBadge.textContent = symbolInput.value; renderSelectedPeriod(); return; }
  historyStatus.hidden = false;
  setLoading(true);
  await loadHistoryPage(requestId, false);
}

async function loadOlderHistory() {
  if (state.historyLoading || !state.historyHasMore || !state.historyBefore || state.marketPeriod === "intraday") return;
  await loadHistoryPage(state.marketRequestId, true);
}

async function loadHistoryPage(requestId, older) {
  state.historyLoading = true;
  state.historyFailed = false;
  historyStatus.textContent = older ? "正在加载更早行情…" : "正在加载行情…";
  loadOlderButton.hidden = true;
  const params = new URLSearchParams({ symbol: state.historySymbol, period: state.marketPeriod, adjustment_type: MARKET_ADJUSTMENT });
  if (older) params.set("before", state.historyBefore);
  let timedOut = false;
  const controller = state.marketAbortController;
  const timeout = setTimeout(() => { timedOut = true; controller.abort(); }, 60000);
  try {
    let response = await fetch(`/api/market/history?${params.toString()}`, { signal: state.marketAbortController.signal });
    let payload = await response.json().catch(() => ({}));
    if (requestId !== state.marketRequestId) return;
    if (!response.ok) throw new Error(payload.detail || `行情查询失败（${response.status}）`);
    if (!older && !payload.items?.length && payload.has_more === false && state.historySymbol !== symbolInput.value.trim()) {
      params.set("symbol", symbolInput.value.trim());
      response = await fetch(`/api/market/history?${params.toString()}`, { signal: state.marketAbortController.signal });
      payload = await response.json().catch(() => ({}));
      if (requestId !== state.marketRequestId) return;
      if (!response.ok) throw new Error(payload.detail || `行情查询失败（${response.status}）`);
      state.historySymbol = symbolInput.value.trim();
    }
    const summary = payload.items?.[0];
    let viewport = null;
    if (older && state.chart && state.historyBars.length) {
      viewport = currentChartViewport(aggregateBars(displayDailyBars(), state.marketPeriod));
    }
    const merged = new Map(state.historyBars.map((bar) => [bar.trading_date, bar]));
    for (const bar of summary?.bars || []) if (!merged.has(bar.trading_date)) merged.set(bar.trading_date, bar);
    state.historyBars = [...merged.values()].sort((a, b) => a.trading_date.localeCompare(b.trading_date));
    state.historyBefore = payload.next_before;
    state.historyHasMore = payload.has_more === true;
    if (!older && summary) {
      state.marketSummary = summary;
      marketSourceBadge.textContent = `${summary.symbol} · ${summary.exchange}`;
    } else if (!older) marketSourceBadge.textContent = symbolInput.value;
    state.historyReady = true;
    if (!older) state.lastReconcileAt = Date.now();
    renderHistoryChart(viewport);
    state.historyAvailability = payload.has_more === null ? {
      symbol: state.historySymbol, period: state.marketPeriod, before: payload.next_before, snapshot_id: payload.snapshot_id,
      table_uuid: payload.table_uuid, query_time: payload.query_time,
      initialEmpty: !older && !summary,
    } : null;
    state.historyCheckFailed = false;
    state.historyCheckExpired = false;
    historyStatus.textContent = payload.has_more === null ? "正在检查更早行情…" : payload.has_more ? "向左拖动查看更早行情" : "已到最早可用行情";
    loadOlderButton.hidden = payload.has_more !== true;
    loadOlderButton.textContent = "加载更早行情";
    setMarketMessage("");
    if (state.historyAvailability) checkHistoryAvailability(requestId, state.historyAvailability);
  } catch (error) {
    if ((error.name === "AbortError" && !timedOut) || requestId !== state.marketRequestId) return;
    state.historyFailed = true;
    historyStatus.textContent = "行情加载失败，请重试";
    loadOlderButton.hidden = false;
    loadOlderButton.textContent = "重试";
    if (!older) { marketSourceBadge.textContent = "查询失败"; setMarketMessage(timedOut ? "行情查询超时，请稍后重试。" : (error.message || "行情查询失败，请稍后重试。"), "error"); }
  } finally {
    clearTimeout(timeout);
    if (requestId === state.marketRequestId) { state.historyLoading = false; setLoading(false); }
  }
}

async function checkHistoryAvailability(requestId, context) {
  if (state.historyChecking) return;
  state.historyChecking = true;
  state.historyCheckFailed = false;
  historyStatus.textContent = "正在检查更早行情…";
  loadOlderButton.hidden = true;
  const controller = new AbortController();
  const parent = state.marketAbortController.signal;
  const abort = () => controller.abort();
  parent.addEventListener("abort", abort, { once: true });
  const timeout = setTimeout(abort, 60000);
  try {
    const params = new URLSearchParams({ symbol: context.symbol, before: context.before,
      adjustment_type: MARKET_ADJUSTMENT, period: context.period, snapshot_id: context.snapshot_id,
      table_uuid: context.table_uuid, query_time: context.query_time });
    const response = await fetch(`/api/market/history/availability?${params}`, { signal: controller.signal });
    if (!response.ok) {
      const error = new Error(`HTTP ${response.status}`);
      error.snapshotExpired = response.status === 410;
      throw error;
    }
    const payload = await response.json();
    if (requestId !== state.marketRequestId || state.historyAvailability !== context) return;
    state.historyHasMore = payload.has_more === true;
    state.historyAvailability = null;
    if (context.initialEmpty && !state.historyHasMore && context.symbol !== symbolInput.value.trim()) {
      state.historySymbol = symbolInput.value.trim();
      state.historyChecking = false;
      await loadHistoryPage(requestId, false);
      return;
    }
    historyStatus.textContent = state.historyHasMore ? "向左拖动查看更早行情" : "已到最早可用行情";
    loadOlderButton.hidden = !state.historyHasMore;
    loadOlderButton.textContent = "加载更早行情";
  } catch (error) {
    if (requestId !== state.marketRequestId || parent.aborted || state.historyAvailability !== context) return;
    state.historyCheckFailed = true;
    state.historyCheckExpired = Boolean(error.snapshotExpired);
    historyStatus.textContent = state.historyCheckExpired ? "行情数据已更新，请重新加载" : "检查更早行情失败，请重试";
    loadOlderButton.hidden = false;
    loadOlderButton.textContent = state.historyCheckExpired ? "重新加载" : "重试检查";
  } finally {
    clearTimeout(timeout);
    parent.removeEventListener("abort", abort);
    if (requestId === state.marketRequestId && (!state.historyAvailability || state.historyAvailability === context)) state.historyChecking = false;
  }
}

async function reconcileFormalBar() {
  const target = state.lastProvisionalDate;
  if (!target || state.reconciling || !state.historyReady || state.historyBars.some((bar) => bar.trading_date === target) || Date.now() - state.lastReconcileAt < 120000) return;
  const requestId = state.marketRequestId;
  const period = state.marketPeriod;
  const params = new URLSearchParams({ symbol: state.historySymbol, period, adjustment_type: MARKET_ADJUSTMENT });
  state.reconciling = true;
  state.lastReconcileAt = Date.now();
  try {
    const response = await fetch(`/api/market/history?${params}`, { signal: state.marketAbortController.signal });
    if (!response.ok) return;
    const data = await response.json();
    if (requestId !== state.marketRequestId || period !== state.marketPeriod) return;
    const formal = data.items?.[0]?.bars?.find((bar) => bar.trading_date === target);
    if (!formal) return;
    const viewport = currentChartViewport(aggregateBars(displayDailyBars(), period));
    state.historyBars = [...state.historyBars.filter((bar) => bar.trading_date !== target), formal].sort((a, b) => a.trading_date.localeCompare(b.trading_date));
    state.lastProvisionalDate = null;
    renderHistoryChart(viewport);
  } catch (error) {
    if (error.name !== "AbortError") console.warn("日线更新暂不可用");
  } finally {
    state.reconciling = false;
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
    addMessage("assistant", "你好！你可以问我股票行情、财经新闻和公司年报相关的问题。例如：贵州茅台最近一个月的行情如何？");
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
  chatProgressTitle.textContent = "正在准备回答";
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
    if (citation.content_type === "summary") item.textContent += " · 摘要";
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
    const connectedItem = appendChatProgress("已准备回答", "正在确定需要的资料");
    finishChatProgressItem(connectedItem);
    setChatProgressTitle("正在查找相关资料");
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
          generatingItem = appendChatProgress("正在整理回答", "根据已取得的资料组织回答");
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
        streamError = payload.error || "回答失败，请重试";
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
    if (!answerText) addMessage("assistant", "暂时无法生成回答，请重试。");
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
    return `<article class="news-card"><h3>${href}</h3><p>${item.provider === "caixin" && !item.content ? "摘要：" : ""}${escapeHtml(formatNewsExcerpt(item))}</p><div class="news-time"><span>${escapeHtml(item.source_name || item.provider || "未知来源")}</span><span>${escapeHtml(formatDateTime(item.published_at))}</span></div><div class="news-tags">${tags}</div></article>`;
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
    reportList.innerHTML = '<div class="empty-state">没有找到符合条件的年报，请调整筛选条件。</div>';
    return;
  }
  const extractionStatusLabel = {
    pending_parse: "PDF 可查看 · 文本待处理",
    needs_review: "PDF 可查看 · 文本待复核",
    ok: "PDF 可查看 · 文本已提取",
  };
  reportList.innerHTML = items.map((report) => `
    <article class="report-card" data-report-id="${escapeHtml(report.report_id)}">
      <h3>${escapeHtml(report.title)}</h3>
      <div class="report-meta"><span>${escapeHtml(report.stock_code)} · ${escapeHtml(report.company_name || "公司名称未知")}</span><span>${escapeHtml(String(report.report_year))} 年报</span><span>${escapeHtml(formatDateTime(report.announcement_time))}</span><span>${escapeHtml(extractionStatusLabel[report.extraction_status] || report.extraction_status || "未知状态")}</span></div>
      <div class="report-actions"><a href="/api/reports/${encodeURIComponent(report.report_id)}/download" target="_blank" rel="noreferrer">打开 PDF</a></div>
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
    if (!response.ok) throw new Error(payload.detail || `年报加载失败（${response.status}）`);
    renderReports(payload.items || []);
    reportSummary.textContent = `共 ${payload.total || 0} 份年报`;
    reportPageInfo.textContent = `第 ${payload.page || state.reportPage} 页`;
    prevReportPageButton.disabled = (payload.page || state.reportPage) <= 1;
    nextReportPageButton.disabled = !payload.has_next;
  } catch (error) {
    reportList.innerHTML = `<div class="empty-state">${escapeHtml(error.message || "年报加载失败")}</div>`;
    reportSummary.textContent = "加载失败";
    prevReportPageButton.disabled = true;
    nextReportPageButton.disabled = true;
  } finally {
    refreshReportsButton.disabled = false;
  }
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

reportFilterForm.addEventListener("submit", (event) => {
  event.preventDefault();
  state.reportPage = 1;
  loadReports();
});
refreshReportsButton.addEventListener("click", loadReports);
prevReportPageButton.addEventListener("click", () => { if (state.reportPage > 1) { state.reportPage -= 1; loadReports(); } });
nextReportPageButton.addEventListener("click", () => { state.reportPage += 1; loadReports(); });
marketForm.addEventListener("submit", (event) => event.preventDefault());
let securityMatches = [];
let securityActive = 0;
function closeSecurityResults() {
  securityResults.hidden = true;
  securitySearch.setAttribute("aria-expanded", "false");
  securitySearch.removeAttribute("aria-activedescendant");
}
function showSecurityResults(query) {
  const text = query.trim().toLocaleLowerCase();
  const all = state.securities.map((item, index) => ({ item, index })).filter(({ item }) => `${item.symbol} ${item.name} ${item.exchange}`.toLocaleLowerCase().includes(text));
  securityMatches = all;
  securityActive = 0;
  securityResults.innerHTML = all.map(({ item, index }, position) => `<div class="security-option" role="option" id="security-option-${position}" data-index="${index}" aria-selected="${position === 0}"><span>${escapeHtml(item.name)}</span><small>${escapeHtml(item.symbol)} · ${escapeHtml(item.exchange)}</small></div>`).join("") + (!all.length ? '<div class="security-results-note">未找到证券</div>' : "");
  securityResults.hidden = false;
  securitySearch.setAttribute("aria-expanded", "true");
  if (securityMatches.length) securitySearch.setAttribute("aria-activedescendant", "security-option-0");
  else securitySearch.removeAttribute("aria-activedescendant");
}
function chooseSecurity(index) {
  const item = state.securities[index];
  if (!item) return;
  symbolInput.selectedIndex = index;
  securitySearch.value = `${item.name} · ${item.symbol}`;
  closeSecurityResults();
  symbolInput.dispatchEvent(new Event("change", { bubbles: true }));
}
securitySearch.addEventListener("focus", () => { securitySearch.select(); showSecurityResults(symbolInput.value ? "" : securitySearch.value); });
securitySearch.addEventListener("click", () => { if (securityResults.hidden) showSecurityResults(symbolInput.value ? "" : securitySearch.value); });
securitySearch.addEventListener("blur", closeSecurityResults);
securitySearch.addEventListener("input", () => {
  symbolInput.selectedIndex = -1;
  showSecurityResults(securitySearch.value);
  ++state.marketRequestId;
  ++state.intradayRequestId;
  state.marketAbortController?.abort();
  state.historyHasMore = false;
  state.historyBars = [];
  state.historyReady = false;
  state.liveQuote = null;
  state.lastProvisionalDate = null;
  clearMarket();
  state.chart?.clear();
  chartEmpty.hidden = false;
  chartEmpty.textContent = "请选择证券";
  chartSubtitle.textContent = "暂无数据";
  historyStatus.textContent = "";
  loadOlderButton.hidden = true;
  marketSourceBadge.textContent = "请选择证券";
  setMarketMessage("");
  clearStockPrice();
  stockPriceTime.textContent = "请选择证券";
  clearStockMetrics();
  loadRealtimeQuotes();
});
securitySearch.addEventListener("keydown", (event) => {
  if (event.key === "Escape") { closeSecurityResults(); return; }
  if (!["ArrowDown", "ArrowUp", "Enter"].includes(event.key)) return;
  event.preventDefault();
  if (securityResults.hidden) { showSecurityResults(symbolInput.value ? "" : securitySearch.value); return; }
  if (!securityMatches.length) return;
  if (event.key === "Enter") { chooseSecurity(securityMatches[securityActive].index); return; }
  securityActive = (securityActive + (event.key === "ArrowDown" ? 1 : -1) + securityMatches.length) % securityMatches.length;
  for (const [index, option] of [...securityResults.querySelectorAll('[role="option"]')].entries()) option.setAttribute("aria-selected", String(index === securityActive));
  const active = document.getElementById(`security-option-${securityActive}`);
  securitySearch.setAttribute("aria-activedescendant", active.id);
  active.scrollIntoView({ block: "nearest" });
});
securityResults.addEventListener("mousedown", (event) => event.preventDefault());
securityResults.addEventListener("click", (event) => {
  const option = event.target.closest("[data-index]");
  if (option) chooseSecurity(Number(option.dataset.index));
});
document.addEventListener("click", (event) => { if (!event.target.closest(".security-picker")) closeSecurityResults(); });
marketForm.addEventListener("change", (event) => {
  if (event.target.matches("select")) loadMarket();
  if (event.target === symbolInput) {
    clearStockPrice();
    stockPriceTime.textContent = "正在获取行情";
    clearStockMetrics();
    loadRealtimeQuotes();
  }
});
document.querySelector(".period-tabs").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-period]");
  if (!button) return;
  state.marketPeriod = button.dataset.period;
  for (const tab of document.querySelectorAll(".period-tabs button")) {
    const active = tab === button;
    tab.classList.toggle("active", active);
    tab.setAttribute("aria-selected", String(active));
  }
  loadMarket();
});
loadOlderButton.addEventListener("click", () => {
  if (state.historyCheckExpired) loadMarket();
  else if (state.historyCheckFailed && state.historyAvailability) checkHistoryAvailability(state.marketRequestId, state.historyAvailability);
  else if (state.historyBefore) loadOlderHistory();
  else loadMarket();
});
chatForm.addEventListener("submit", (event) => { event.preventDefault(); const question = questionInput.value.trim(); if (!question || state.chatStreaming) return; addMessage("user", question); questionInput.value = ""; sendQuestion(question); });
questionInput.addEventListener("keydown", (event) => { if (event.key === "Enter" && (event.ctrlKey || event.metaKey)) chatForm.requestSubmit(); });
refreshNewsButton.addEventListener("click", loadNews);
pageSizeSelect.addEventListener("change", () => { state.newsPage = 1; state.newsPageSize = Number(pageSizeSelect.value); loadNews(); });
prevPageButton.addEventListener("click", () => { if (state.newsPage > 1) { state.newsPage -= 1; loadNews(); } });
nextPageButton.addEventListener("click", () => { state.newsPage += 1; loadNews(); });
window.addEventListener("resize", () => state.chart?.resize());

let realtimeRequestId = 0;
let realtimeAbortController = null;
function formatRealtimeTime(value) {
  if (!value) return "未知";
  const timestamp = new Date(value);
  if (Number.isNaN(timestamp.getTime())) return "未知";
  return timestamp.toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false });
}
const indexNames = [["000001.SH", "上证指数"], ["399001.SZ", "深证成指"], ["399006.SZ", "创业板指"]];
function renderRealtimeIndices(indices = []) {
  const bySymbol = new Map(indices.map((item) => [item.symbol, item]));
  document.getElementById("realtimeIndices").innerHTML = indexNames.map(([symbol, name]) => {
    const item = bySymbol.get(symbol);
    if (!item) return `<div class="index-card"><span class="index-name">${name}</span><strong>—</strong></div>`;
    const change = Number(item.change);
    const color = change > 0 ? "market-up" : change < 0 ? "market-down" : "market-flat";
    const sign = change > 0 ? "+" : "";
    return `<div class="index-card"><span class="index-name">${name}</span><strong class="${color}">${formatNumber(item.price)}</strong><span class="index-change ${color}">${sign}${formatNumber(change)}　${sign}${formatNumber(item.change_percent)}%</span><small>${escapeHtml(formatRealtimeTime(item.source_time))}</small></div>`;
  }).join("");
}
renderRealtimeIndices();
function clearStockMetrics() {
  stockVolume.textContent = stockMarketCap.textContent = stockPe.textContent = "—";
}
function clearStockPrice() {
  stockLastPrice.textContent = "—";
  stockLastPrice.className = "";
  stockChangePercent.textContent = "";
  stockChangePercent.className = "stock-change";
  stockPreviousClose.textContent = "—";
}
function renderStockMetrics(quote) {
  const volume = quote?.volume_lots;
  const cap = quote?.market_cap_cny;
  const pe = quote?.pe_ratio;
  stockVolume.textContent = volume == null ? "—" : Number(volume) >= 10000 ? `${formatNumber(Number(volume) / 10000)} 万手` : `${formatNumber(volume, 0)} 手`;
  stockMarketCap.textContent = cap == null ? "—" : `${formatNumber(Number(cap) / 100000000)} 亿元`;
  stockPe.textContent = pe == null || Number(pe) === 0 ? "—" : Number(pe) < 0 ? "亏损" : `${formatNumber(pe)} 倍`;
}
async function loadRealtimeQuotes() {
  const overview = document.getElementById("realtimeMarketOverview");
  const requestId = ++realtimeRequestId;
  const symbol = symbolInput.value;
  const exchange = symbolInput.selectedOptions[0]?.dataset.exchange?.toUpperCase();
  const quoteSymbol = /^\d{6}$/.test(symbol) && ["SH", "SZ", "BJ"].includes(exchange) ? `${symbol}.${exchange}` : symbol;
  if (realtimeAbortController) realtimeAbortController.abort();
  realtimeAbortController = new AbortController();
  try {
    const params = new URLSearchParams();
    if (quoteSymbol) params.set("symbol", quoteSymbol);
    const response = await fetch(`/api/realtime-quotes?${params}`, { signal: realtimeAbortController.signal });
    if (!response.ok) throw new Error("unavailable");
    const data = await response.json();
    if (requestId !== realtimeRequestId || symbol !== symbolInput.value) return;
    const quote = data.quote;
    const updateChart = state.marketPeriod !== "intraday" && state.historyReady && !state.historyLoading;
    const previousBars = updateChart ? aggregateBars(displayDailyBars(), state.marketPeriod) : [];
    const previousLive = updateChart ? JSON.stringify(provisionalDailyBar(state.liveQuote, state.historyBars, selectedQuoteSymbol(), Date.now())) : null;
    const viewport = updateChart ? currentChartViewport(previousBars) : null;
    const atLatest = updateChart && (state.chart?.getOption().dataZoom?.[0]?.end ?? 0) >= 99.9;
    state.liveQuote = quote;
    if (updateChart) {
      const nextBar = provisionalDailyBar(quote, state.historyBars, selectedQuoteSymbol(), Date.now());
      const nextLive = JSON.stringify(nextBar);
      if (viewport && atLatest && nextBar) viewport.endValue = nextBar.trading_date;
      if (previousLive !== nextLive) renderHistoryChart(viewport);
      reconcileFormalBar();
    }
    if (quote?.price != null && Number.isFinite(Number(quote.price))) {
      stockLastPrice.textContent = formatNumber(quote.price);
      const previous = Number(quote.previous_close);
      const price = Number(quote.price);
      const comparable = quote.previous_close != null && Number.isFinite(previous) && previous > 0;
      stockPreviousClose.textContent = comparable ? formatNumber(previous) : "—";
      const change = comparable ? (price - previous) / previous * 100 : 0;
      const color = comparable && change > 0 ? "market-up" : comparable && change < 0 ? "market-down" : "";
      stockLastPrice.className = color;
      stockChangePercent.className = `stock-change ${color}`;
      stockChangePercent.textContent = comparable ? `${change > 0 ? "+" : ""}${formatNumber(change)}%` : "";
      stockPriceTime.textContent = `${quote.status === "fresh" ? "" : "最近行情 · "}${formatRealtimeTime(quote.source_time)}`;
      renderStockMetrics(quote);
    } else {
      clearStockPrice();
      stockPriceTime.textContent = symbol ? "暂无行情" : "请选择证券";
      clearStockMetrics();
    }
    const market = data.market;
    renderRealtimeIndices(data.indices || []);
    overview.innerHTML = market
      ? [["上涨", market.rising, "market-up"], ["下跌", market.falling, "market-down"], ["平盘", market.flat, "market-flat"]].map(([label, value, color]) => `<span class="breadth-item">${label}<strong class="${color}">${escapeHtml(String(value))}</strong></span>`).join("") + `<span class="market-meta">${escapeHtml(formatRealtimeTime(market.round_id))}</span>`
      : "";
  } catch (error) {
    if (error.name === "AbortError" || requestId !== realtimeRequestId) return;
    const updateChart = state.marketPeriod !== "intraday" && state.historyReady && !state.historyLoading;
    const viewport = updateChart ? currentChartViewport(aggregateBars(displayDailyBars(), state.marketPeriod)) : null;
    state.liveQuote = null;
    if (updateChart) renderHistoryChart(viewport);
    clearStockPrice();
    stockPriceTime.textContent = "行情暂不可用";
    clearStockMetrics();
    renderRealtimeIndices();
    overview.textContent = "";
  }
}
loadRealtimeQuotes();
setInterval(() => { loadRealtimeQuotes(); if (state.marketPeriod === "intraday") loadIntraday(); }, 15000);
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
