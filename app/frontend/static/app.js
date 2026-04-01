(function () {
  "use strict";

  const painPhaseEl = document.getElementById("pain-phase");
  const dominantGroupEl = document.getElementById("dominant-group");
  const nextGroupEl = document.getElementById("next-group");
  const guidanceEl = document.getElementById("guidance");
  const confidenceTextEl = document.getElementById("confidence-text");
  const confidenceFillEl = document.getElementById("confidence-fill");
  const explanationEl = document.getElementById("explanation");
  const timelineEl = document.getElementById("timeline");
  const featureListEl = document.getElementById("feature-list");
  const apiStatusEl = document.getElementById("api-status");
  const streamStatusEl = document.getElementById("stream-status");
  const readyStatusEl = document.getElementById("ready-status");
  const readySummaryEl = document.getElementById("ready-summary");
  const userLineEl = document.getElementById("user-line");
  const lastUpdateEl = document.getElementById("last-update");
  const clearLogBtn = document.getElementById("clear-log");
  const logoutBtn = document.getElementById("logout-btn");
  const paperBackendEl = document.getElementById("paper-backend");
  const paperClosedEl = document.getElementById("paper-closed");
  const paperProfitEl = document.getElementById("paper-profit");
  const paperLossEl = document.getElementById("paper-loss");
  const paperWinRateEl = document.getElementById("paper-winrate");
  const paperNetEl = document.getElementById("paper-net");
  const paperActiveTradeEl = document.getElementById("paper-active-trade");
  const paperTradesBodyEl = document.getElementById("paper-trades-body");
  const paperGateLastEl = document.getElementById("paper-gate-last");
  const paperGateListEl = document.getElementById("paper-gate-list");
  const paperResetBtn = document.getElementById("paper-reset-btn");
  const fyersStatusBtn = document.getElementById("fyers-status-btn");
  const fyersUrlBtn = document.getElementById("fyers-url-btn");
  const fyersRefreshBtn = document.getElementById("fyers-refresh-btn");
  const fyersExchangeBtn = document.getElementById("fyers-exchange-btn");
  const fyersAuthCodeEl = document.getElementById("fyers-auth-code");
  const fyersStatusViewEl = document.getElementById("fyers-status-view");
  const fyersMsgEl = document.getElementById("fyers-msg");
  const hasFyersUi = Boolean(
    fyersStatusBtn ||
    fyersUrlBtn ||
    fyersRefreshBtn ||
    fyersExchangeBtn ||
    fyersStatusViewEl ||
    fyersMsgEl
  );
  const chartEl = document.getElementById("live-chart");
  const chartStatusEl = document.getElementById("chart-status");
  const tf1mBtn = document.getElementById("tf-1m");
  const tf5mBtn = document.getElementById("tf-5m");
  const hasChartUi = Boolean(chartEl && chartStatusEl);
  const backtestRunBtn = document.getElementById("backtest-run-btn");
  const backtestLoadBtn = document.getElementById("backtest-load-btn");
  const backtestStartEl = document.getElementById("backtest-start");
  const backtestEndEl = document.getElementById("backtest-end");
  const backtestMaxCandlesEl = document.getElementById("backtest-max-candles");
  const backtestMaxTradesEl = document.getElementById("backtest-max-trades");
  const backtestCloseOpenEl = document.getElementById("backtest-close-open");
  const backtestProgressFillEl = document.getElementById("backtest-progress-fill");
  const backtestProgressTextEl = document.getElementById("backtest-progress-text");
  const backtestStatusEl = document.getElementById("backtest-status");
  const backtestCountEl = document.getElementById("backtest-count");
  const backtestWinRateEl = document.getElementById("backtest-winrate");
  const backtestNetEl = document.getElementById("backtest-net");
  const backtestProfitFactorEl = document.getElementById("backtest-profit-factor");
  const backtestMaxDdEl = document.getElementById("backtest-maxdd");
  const backtestGrossProfitEl = document.getElementById("backtest-gross-profit");
  const backtestGrossLossEl = document.getElementById("backtest-gross-loss");
  const backtestWinEl = document.getElementById("backtest-win");
  const backtestLossEl = document.getElementById("backtest-loss");
  const backtestFlatEl = document.getElementById("backtest-flat");
  const backtestLongEl = document.getElementById("backtest-long");
  const backtestShortEl = document.getElementById("backtest-short");
  const backtestReasonListEl = document.getElementById("backtest-reason-list");
  const backtestTradesBodyEl = document.getElementById("backtest-trades-body");
  const retrainRefreshBtn = document.getElementById("retrain-refresh-btn");
  const retrainStatusViewEl = document.getElementById("retrain-status-view");
  const retrainHistoryBodyEl = document.getElementById("retrain-history-body");
  const retrainMistakeSummaryEl = document.getElementById("retrain-mistake-summary");
  const retrainMistakeBodyEl = document.getElementById("retrain-mistake-body");
  const sidebarToggleBtn = document.getElementById("sidebar-toggle");
  const sidebarCloseBtn = document.getElementById("sidebar-close");
  const sidebarOverlayEl = document.getElementById("sidebar-overlay");
  const sidebarNavLinks = Array.from(document.querySelectorAll("[data-nav-close]"));
  const viewNavButtons = Array.from(document.querySelectorAll("[data-view-target]"));
  const portalViews = Array.from(document.querySelectorAll("[data-view]"));
  const profileWrapEl = document.getElementById("profile-wrap");
  const profileToggleBtn = document.getElementById("profile-toggle-btn");
  const profileMenuEl = document.getElementById("profile-menu");
  const profileLogoutBtn = document.getElementById("profile-logout-btn");
  const profileNameEl = document.getElementById("profile-name");
  const profileEmailEl = document.getElementById("profile-email");
  const profileMobileEl = document.getElementById("profile-mobile");
  const profilePlanEl = document.getElementById("profile-plan");
  const profileLotSizeQtyEl = document.getElementById("profile-lot-size-qty");
  const profileLotCountEl = document.getElementById("profile-lot-count");
  const profileOrderQtyEl = document.getElementById("profile-order-qty");
  const userLotCountInputEl = document.getElementById("user-lot-count-input");
  const userLotCountSaveBtn = document.getElementById("user-lot-count-save-btn");
  const userLotCountMsgEl = document.getElementById("user-lot-count-msg");
  const profileAvatarTextEl = document.getElementById("profile-avatar-text");
  const adminLotSizeInputEl = document.getElementById("admin-lot-size-input");
  const adminLotSizeSaveBtn = document.getElementById("admin-lot-size-save-btn");
  const adminLotSizeCurrentEl = document.getElementById("admin-lot-size-current");
  const adminLotSizeMsgEl = document.getElementById("admin-lot-size-msg");
  const angelApiDateEl = document.getElementById("angel-api-date");
  const angelApiLoadBtn = document.getElementById("angel-api-load-btn");
  const angelApiStatusEl = document.getElementById("angel-api-status");
  const angelApiDayLabelEl = document.getElementById("angel-api-day-label");
  const angelApiTotalEl = document.getElementById("angel-api-total");
  const angelApiSuccessEl = document.getElementById("angel-api-success");
  const angelApiFailedEl = document.getElementById("angel-api-failed");
  const angelApiUsersEl = document.getElementById("angel-api-users");
  const angelApiCountEl = document.getElementById("angel-api-count");
  const angelApiUserListEl = document.getElementById("angel-api-user-list");
  const angelApiBodyEl = document.getElementById("angel-api-body");
  const brokerClientIdValueEl = document.getElementById("broker-client-id-value");
  const brokerApiKeyValueEl = document.getElementById("broker-api-key-value");
  const brokerPinValueEl = document.getElementById("broker-pin-value");
  const brokerTotpValueEl = document.getElementById("broker-totp-value");
  const brokerClientIdInputEl = document.getElementById("broker-client-id-input");
  const brokerApiKeyInputEl = document.getElementById("broker-api-key-input");
  const brokerPinInputEl = document.getElementById("broker-pin-input");
  const brokerTotpInputEl = document.getElementById("broker-totp-input");
  const brokerClientIdSaveBtn = document.getElementById("broker-client-id-save-btn");
  const brokerClientIdMsgEl = document.getElementById("broker-client-id-msg");
  const terminalLoginToggleEl = document.getElementById("terminal-login-toggle");
  const tradingEngineToggleEl = document.getElementById("trading-engine-toggle");
  const tradingEngineMsgEl = document.getElementById("trading-engine-msg");
  const subscribePlanSelectEl = document.getElementById("subscribe-plan-select");
  const subscribePlanPriceEl = document.getElementById("subscribe-plan-price");
  const subscribeNowBtn = document.getElementById("subscribe-now-btn");
  const subscribeModalOpenBtnEl = document.getElementById("subscribe-modal-open-btn");
  const subscribeModalEl = document.getElementById("subscribe-modal");
  const subscribeModalCloseBtnEl = document.getElementById("subscribe-modal-close-btn");
  const subscribeModalCloseEls = Array.from(document.querySelectorAll("[data-subscribe-modal-close]"));
  const subscribePlanButtons = Array.from(document.querySelectorAll("[data-subscribe-plan]"));
  const subscribePlanCardEls = Array.from(document.querySelectorAll("[data-plan-card]"));
  const subscribeMsgEl = document.getElementById("subscribe-msg");
  const reportStartDateEl = document.getElementById("report-start-date");
  const reportEndDateEl = document.getElementById("report-end-date");
  const reportLoadBtn = document.getElementById("report-load-btn");
  const reportModeSelectEl = document.getElementById("report-mode-select");
  const reportStatusEl = document.getElementById("report-status");
  const reportCountEl = document.getElementById("report-count");
  const reportTradesBodyEl = document.getElementById("report-trades-body");

  let lastSnapshot = null;
  let lastMarket = null;
  let timeline = [];
  let chartTf = "1m";
  let chart = null;
  let candleSeries = null;
  let tickSeries = null;
  let tickTrail = [];
  let chartFitted = false;
  let resizeObserver = null;
  let chartHistoryLoaded = false;
  let chartLastBarTime = 0;
  let lastTickTime = 0;
  let marketBusy = false;
  let coreBusy = false;
  let readyBusy = false;
  let chartAutoSized = false;
  let activeBacktestJobId = "";
  let retrainBusy = false;
  let brokerBusy = false;
  let terminalConnected = false;
  let terminalToggleBusy = false;
  let tradingEngineEnabled = false;
  let tradingEngineToggleBusy = false;
  let subscriptionPaidActive = false;
  let subscriptionExpiresAt = 0;
  let subscriptionPlanName = "";
  let selectedSubscribePlanCode = "monthly";
  let subscribeBusy = false;
  let currentUser = null;
  let reportBusy = false;
  let lotConfigBusy = false;
  let angelApiBusy = false;
  let angelApiLoadedDate = "";
  let lotSizeQty = 1;
  let userLotCount = 1;
  const SUBSCRIPTION_PLANS = {
    monthly: { label: "Monthly", amount: 3000 },
    quarterly: { label: "Quarterly", amount: 7500 },
    yearly: { label: "Yearly", amount: 24000 },
  };
  const IST_TZ = "Asia/Kolkata";
  const IST_TIME_FMT = new Intl.DateTimeFormat("en-IN", {
    timeZone: IST_TZ,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const IST_TICK_TIME_FMT = new Intl.DateTimeFormat("en-IN", {
    timeZone: IST_TZ,
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const IST_TICK_DATE_TIME_FMT = new Intl.DateTimeFormat("en-IN", {
    timeZone: IST_TZ,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const IST_TICK_PARTS_FMT = new Intl.DateTimeFormat("en-IN", {
    timeZone: IST_TZ,
    day: "2-digit",
    month: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const MOBILE_BREAKPOINT = 1024;

  function isMobileSidebarMode() {
    return window.matchMedia(`(max-width: ${MOBILE_BREAKPOINT}px)`).matches;
  }

  function setSidebarOpen(open) {
    const shouldOpen = Boolean(open) && isMobileSidebarMode();
    document.body.classList.toggle("sidebar-open", shouldOpen);
    if (sidebarToggleBtn) {
      sidebarToggleBtn.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
    }
    if (shouldOpen) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    if (shouldOpen) {
      setProfileMenuOpen(false);
    }
    if (chartEl) {
      setTimeout(function () {
        scheduleChartStabilize();
      }, 240);
    }
  }

  function setProfileMenuOpen(open) {
    if (!profileWrapEl || !profileToggleBtn || !profileMenuEl) {
      return;
    }
    const shouldOpen = Boolean(open);
    profileMenuEl.hidden = !shouldOpen;
    profileWrapEl.classList.toggle("is-open", shouldOpen);
    profileToggleBtn.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
  }

  function syncTopbarScrollState() {
    document.body.classList.toggle("topbar-scrolled", window.scrollY > 8);
  }

  function setActiveView(nextView) {
    const target = String(nextView || "home").trim().toLowerCase() || "home";
    let found = portalViews.some(function (viewEl) {
      return String(viewEl.dataset.view || "").trim().toLowerCase() === target;
    });
    const finalTarget = found ? target : "home";
    portalViews.forEach(function (viewEl) {
      const key = String(viewEl.dataset.view || "").trim().toLowerCase();
      const active = key === finalTarget;
      viewEl.classList.toggle("is-active", active);
      viewEl.hidden = !active;
    });
    viewNavButtons.forEach(function (btn) {
      const key = String(btn.dataset.viewTarget || "").trim().toLowerCase();
      const active = key === finalTarget;
      btn.classList.toggle("is-active", active);
      if (active) {
        btn.setAttribute("aria-current", "page");
      } else {
        btn.removeAttribute("aria-current");
      }
    });
  }

  function hasBrokerUi() {
    return Boolean(
      brokerClientIdValueEl ||
      brokerApiKeyValueEl ||
      brokerPinValueEl ||
      brokerTotpValueEl ||
      brokerClientIdInputEl ||
      brokerApiKeyInputEl ||
      brokerPinInputEl ||
      brokerTotpInputEl ||
      brokerClientIdSaveBtn ||
      brokerClientIdMsgEl ||
      terminalLoginToggleEl
    );
  }

  function hasUserLotUi() {
    return Boolean(
      profileLotSizeQtyEl ||
      profileLotCountEl ||
      profileOrderQtyEl ||
      userLotCountInputEl ||
      userLotCountSaveBtn ||
      userLotCountMsgEl
    );
  }

  function hasAdminLotUi() {
    return Boolean(adminLotSizeInputEl || adminLotSizeSaveBtn || adminLotSizeCurrentEl || adminLotSizeMsgEl);
  }

  function hasAngelApiUi() {
    return Boolean(
      angelApiDateEl &&
      angelApiLoadBtn &&
      angelApiStatusEl &&
      angelApiTotalEl &&
      angelApiSuccessEl &&
      angelApiFailedEl &&
      angelApiUsersEl &&
      angelApiCountEl &&
      angelApiUserListEl &&
      angelApiBodyEl
    );
  }

  function getUserQuantityMultiplier() {
    if (!document.body.classList.contains("user-console")) {
      return 1;
    }
    return Math.max(1, safeNum(lotSizeQty, 1) * safeNum(userLotCount, 1));
  }

  function setLotConfigFromData(configRaw) {
    const cfg = configRaw && typeof configRaw === "object" ? configRaw : {};
    lotSizeQty = Math.max(1, safeNum(cfg.lot_size_qty, lotSizeQty || 1));
    userLotCount = Math.max(1, safeNum(cfg.user_lot_count, userLotCount || 1));
    const orderQty = Math.max(1, safeNum(cfg.order_quantity, lotSizeQty * userLotCount));
    if (profileLotSizeQtyEl) {
      setText(profileLotSizeQtyEl, String(Math.trunc(lotSizeQty)));
    }
    if (profileLotCountEl) {
      setText(profileLotCountEl, String(Math.trunc(userLotCount)));
    }
    if (profileOrderQtyEl) {
      setText(profileOrderQtyEl, String(Math.trunc(orderQty)));
    }
    if (userLotCountInputEl) {
      userLotCountInputEl.value = String(Math.trunc(userLotCount));
    }
  }

  function setTerminalLoginConnected(connected) {
    const isConnected = Boolean(connected);
    terminalConnected = isConnected;
    if (terminalLoginToggleEl) {
      terminalLoginToggleEl.textContent = isConnected ? "On" : "Off";
      terminalLoginToggleEl.classList.toggle("is-on", isConnected);
      terminalLoginToggleEl.classList.toggle("is-off", !isConnected);
      terminalLoginToggleEl.setAttribute("aria-checked", isConnected ? "true" : "false");
    }
  }

  function setTradingEngineEnabled(enabled) {
    const isEnabled = Boolean(enabled) && Boolean(subscriptionPaidActive);
    tradingEngineEnabled = isEnabled;
    if (tradingEngineToggleEl) {
      tradingEngineToggleEl.textContent = isEnabled ? "On" : "Off";
      tradingEngineToggleEl.classList.toggle("is-on", isEnabled);
      tradingEngineToggleEl.classList.toggle("is-off", !isEnabled);
      tradingEngineToggleEl.setAttribute("aria-checked", isEnabled ? "true" : "false");
      if (!tradingEngineToggleBusy) {
        tradingEngineToggleEl.disabled = !subscriptionPaidActive;
      }
    }
    if (tradingEngineMsgEl) {
      if (!subscriptionPaidActive) {
        setText(tradingEngineMsgEl, "Trading engine requires an active paid subscription.");
      } else if (isEnabled && terminalConnected) {
        setText(tradingEngineMsgEl, "Trading engine is on. Live execution is enabled for your account.");
      } else if (isEnabled) {
        setText(tradingEngineMsgEl, "Trading engine is on, but terminal login is disconnected. Live execution is blocked.");
      } else {
        setText(tradingEngineMsgEl, "Trading engine is off. Only paper flow is active.");
      }
    }
  }

  function setSubscriptionStatusFromData(data) {
    const src = data && typeof data === "object" ? data : {};
    subscriptionPaidActive = Boolean(src.subscription_active || src.paid_active);
    subscriptionExpiresAt = safeNum(src.subscription_expires_at, 0);
    subscriptionPlanName = String(src.plan_name || src.subscription_plan_name || "").trim();
    if (profilePlanEl) {
      if (subscriptionPaidActive && subscriptionPlanName) {
        setText(profilePlanEl, subscriptionPlanName);
      }
    }
    if (subscribeModalOpenBtnEl) {
      setText(subscribeModalOpenBtnEl, subscriptionPaidActive ? "Plan Active" : "Subscribe");
      subscribeModalOpenBtnEl.classList.toggle("is-active", subscriptionPaidActive);
    }
  }

  function formatInr(amount) {
    const value = safeNum(amount, 0);
    return `INR ${value.toLocaleString("en-IN")}`;
  }

  function normalizePlanCode(raw) {
    const code = String(raw || "").trim().toLowerCase();
    if (code && Object.prototype.hasOwnProperty.call(SUBSCRIPTION_PLANS, code)) {
      return code;
    }
    return "monthly";
  }

  function getSelectedPlanCode() {
    if (subscribePlanSelectEl) {
      return normalizePlanCode(subscribePlanSelectEl.value);
    }
    return normalizePlanCode(selectedSubscribePlanCode);
  }

  function updateSubscribePlanUi(planCode) {
    const code = normalizePlanCode(planCode || getSelectedPlanCode());
    selectedSubscribePlanCode = code;
    if (subscribePlanSelectEl && subscribePlanSelectEl.value !== code) {
      subscribePlanSelectEl.value = code;
    }
    const plan = SUBSCRIPTION_PLANS[code] || SUBSCRIPTION_PLANS.monthly;
    if (subscribePlanPriceEl) {
      setText(subscribePlanPriceEl, `Selected: ${plan.label} - ${formatInr(plan.amount)}`);
    }
    subscribePlanCardEls.forEach(function (cardEl) {
      const cardCode = normalizePlanCode(cardEl.dataset.planCard || "");
      cardEl.classList.toggle("is-selected", cardCode === code);
    });
  }

  function setSubscribeModalOpen(open) {
    if (!subscribeModalEl) {
      return;
    }
    const shouldOpen = Boolean(open);
    subscribeModalEl.hidden = !shouldOpen;
    subscribeModalEl.setAttribute("aria-hidden", shouldOpen ? "false" : "true");
    document.body.classList.toggle("subscribe-modal-open", shouldOpen);
    if (shouldOpen) {
      setProfileMenuOpen(false);
    }
  }

  function setSubscribeBusyState(isBusy) {
    const busy = Boolean(isBusy);
    if (subscribeNowBtn) {
      subscribeNowBtn.disabled = busy;
    }
    if (subscribeModalOpenBtnEl) {
      subscribeModalOpenBtnEl.disabled = busy;
    }
    subscribePlanButtons.forEach(function (btn) {
      btn.disabled = busy;
    });
  }

  function hasReportUi() {
    return Boolean(reportStartDateEl && reportEndDateEl && reportLoadBtn && reportStatusEl && reportCountEl && reportTradesBodyEl);
  }

  function hasRetrainUi() {
    return Boolean(
      retrainStatusViewEl ||
      retrainHistoryBodyEl ||
      retrainMistakeSummaryEl ||
      retrainMistakeBodyEl ||
      retrainRefreshBtn
    );
  }

  function formatTodayIstDate() {
    const now = new Date();
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: IST_TZ,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(now);
    const map = {};
    parts.forEach(function (part) {
      if (part.type !== "literal") {
        map[part.type] = part.value;
      }
    });
    return `${map.year || "1970"}-${map.month || "01"}-${map.day || "01"}`;
  }

  function shiftDateInput(isoDate, deltaDays) {
    const text = String(isoDate || "").trim();
    const dt = new Date(`${text}T00:00:00`);
    if (Number.isNaN(dt.getTime())) {
      return text;
    }
    dt.setDate(dt.getDate() + deltaDays);
    const y = dt.getFullYear();
    const m = String(dt.getMonth() + 1).padStart(2, "0");
    const d = String(dt.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }

  function setReportDefaults() {
    if (!hasReportUi()) {
      return;
    }
    const today = formatTodayIstDate();
    if (!reportEndDateEl.value) {
      reportEndDateEl.value = today;
    }
    if (!reportStartDateEl.value) {
      reportStartDateEl.value = shiftDateInput(today, -29);
    }
    reportStartDateEl.max = reportEndDateEl.value || today;
    reportEndDateEl.max = today;
    if (reportModeSelectEl && !reportModeSelectEl.value) {
      reportModeSelectEl.value = "all";
    }
  }

  function setAngelApiDefaults() {
    if (!hasAngelApiUi()) {
      return;
    }
    const today = formatTodayIstDate();
    if (!angelApiDateEl.value) {
      angelApiDateEl.value = today;
    }
    angelApiDateEl.max = today;
  }

  function formatJsonForCell(value) {
    if (value === null || value === undefined) {
      return "--";
    }
    if (typeof value === "string") {
      return value || "--";
    }
    try {
      return JSON.stringify(value, null, 2);
    } catch (_err) {
      return String(value);
    }
  }

  function escapeHtml(value) {
    return String(value || "")
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;")
      .replaceAll("'", "&#39;");
  }

  function renderAngelApiUsers(rows) {
    if (!angelApiUserListEl) {
      return;
    }
    const list = Array.isArray(rows) ? rows : [];
    if (!list.length) {
      angelApiUserListEl.innerHTML = '<p class="small">No user stats for selected day.</p>';
      return;
    }
    angelApiUserListEl.innerHTML = list.map(function (row) {
      const username = String(row.username || "--");
      const total = safeNum(row.total_hits, 0);
      const success = safeNum(row.success_hits, 0);
      const failed = safeNum(row.failed_hits, 0);
      return `
        <div class="feature">
          <div class="feature-name">
            <strong>${escapeHtml(username)}</strong>
            <span class="mono">${total} hit(s)</span>
          </div>
          <div class="small">Success ${success} | Failed ${failed}</div>
        </div>
      `;
    }).join("");
  }

  function renderAngelApiRows(rows) {
    if (!angelApiBodyEl) {
      return;
    }
    const data = Array.isArray(rows) ? rows : [];
    if (!data.length) {
      angelApiBodyEl.innerHTML = '<tr><td colspan="10" class="small">No Angel One API hits found for selected day.</td></tr>';
      return;
    }
    angelApiBodyEl.innerHTML = data.map(function (row) {
      const ok = Boolean(row.ok);
      const statusText = ok
        ? `OK${row.http_status ? ` (${row.http_status})` : ""}`
        : `FAIL${row.http_status ? ` (${row.http_status})` : ""}${row.error_message ? ` - ${row.error_message}` : ""}`;
      return `
        <tr>
          <td>${escapeHtml(row.request_time_ist || "--")}</td>
          <td>
            <div>${escapeHtml(row.username || "--")}</div>
            <div class="small mono">${escapeHtml(row.client_id || "--")}</div>
          </td>
          <td>${escapeHtml((row.phase || "--").toUpperCase())}</td>
          <td>${escapeHtml(row.transaction_type || "--")}</td>
          <td>
            <div>${escapeHtml(row.symbol || "--")}</div>
            <div class="small mono">${escapeHtml(row.exchange || "--")} / ${escapeHtml(row.symbol_token || "--")}</div>
          </td>
          <td>${safeNum(row.quantity, 0)}</td>
          <td class="${ok ? "outcome-win" : "outcome-loss"}">${escapeHtml(statusText)}</td>
          <td>${escapeHtml(row.order_id || "--")}</td>
          <td><pre class="tiny-log angel-hit-log">${escapeHtml(formatJsonForCell(row.request_payload))}</pre></td>
          <td><pre class="tiny-log angel-hit-log">${escapeHtml(formatJsonForCell(row.response_payload))}</pre></td>
        </tr>
      `;
    }).join("");
  }

  async function loadAngelApiHits() {
    if (!hasAngelApiUi() || angelApiBusy) {
      return;
    }
    const targetDate = String((angelApiDateEl && angelApiDateEl.value) || "").trim();
    if (!targetDate) {
      setText(angelApiStatusEl, "Date is required.");
      return;
    }
    angelApiBusy = true;
    angelApiLoadBtn.disabled = true;
    setText(angelApiStatusEl, "Loading Angel One API audit log...");
    try {
      const qs = `date=${encodeURIComponent(targetDate)}&limit=1000`;
      const payload = await requestJson(`/admin/execution/angel-api-hits?${qs}`);
      const summary = payload && typeof payload.summary === "object" ? payload.summary : {};
      const rows = Array.isArray(payload && payload.rows) ? payload.rows : [];
      angelApiLoadedDate = String(payload.date || targetDate);
      setText(angelApiDayLabelEl, angelApiLoadedDate || "--");
      setText(angelApiTotalEl, String(safeNum(summary.total_hits, 0)));
      setText(angelApiSuccessEl, String(safeNum(summary.success_hits, 0)));
      setText(angelApiFailedEl, String(safeNum(summary.failed_hits, 0)));
      setText(angelApiUsersEl, String(safeNum(summary.unique_users, 0)));
      setText(angelApiCountEl, String(safeNum(payload.count, rows.length)));
      renderAngelApiUsers(summary.users);
      renderAngelApiRows(rows);
      setText(
        angelApiStatusEl,
        `Loaded ${safeNum(summary.total_hits, 0)} Angel One place-order hit(s) for ${angelApiLoadedDate}.`
      );
    } catch (err) {
      renderAngelApiUsers([]);
      renderAngelApiRows([]);
      setText(angelApiTotalEl, "0");
      setText(angelApiSuccessEl, "0");
      setText(angelApiFailedEl, "0");
      setText(angelApiUsersEl, "0");
      setText(angelApiCountEl, "0");
      setText(angelApiDayLabelEl, targetDate || "--");
      setText(angelApiStatusEl, `Angel API log load failed: ${err.message}`);
    } finally {
      angelApiBusy = false;
      angelApiLoadBtn.disabled = false;
    }
  }

  function reportOutcomeClass(outcome) {
    const o = String(outcome || "").toLowerCase();
    if (o === "win") return "outcome-win";
    if (o === "loss") return "outcome-loss";
    if (o === "open") return "outcome-open";
    return "";
  }

  function reportModeLabel(mode) {
    return String(mode || "").toLowerCase() === "live" ? "Live" : "Paper";
  }

  function renderReportRows(rows) {
    if (!reportTradesBodyEl) {
      return;
    }
    const data = Array.isArray(rows) ? rows : [];
    if (!data.length) {
      reportTradesBodyEl.innerHTML = '<tr><td colspan="9" class="small">No trades found for selected range.</td></tr>';
      return;
    }
    reportTradesBodyEl.innerHTML = data.map(function (row) {
      const modeLabel = reportModeLabel(row.mode);
      const modeCls = String(row.mode || "").toLowerCase() === "live" ? "mode-live" : "mode-paper";
      const entryPrice = (row.entry_price !== null && row.entry_price !== undefined) ? safeNum(row.entry_price, 0).toFixed(2) : "--";
      const exitPrice = (row.exit_price !== null && row.exit_price !== undefined && row.exit_price > 0) ? safeNum(row.exit_price, 0).toFixed(2) : "--";
      const points = (row.points !== null && row.points !== undefined) ? (safeNum(row.points, 0) >= 0 ? "+" : "") + safeNum(row.points, 0).toFixed(2) : "--";
      const outcomeCls = reportOutcomeClass(row.outcome);
      return `
        <tr>
          <td><span class="mode-badge ${modeCls}">${modeLabel}</span></td>
          <td>${String(row.entry_time_ist || "--")}</td>
          <td>${String(row.exit_time_ist || "--")}</td>
          <td>${String(row.symbol || "--")}</td>
          <td>${String(row.direction || "--")}</td>
          <td>${entryPrice}</td>
          <td>${exitPrice}</td>
          <td>${points}</td>
          <td class="${outcomeCls}">${String(row.outcome || "--")}</td>
        </tr>
      `;
    }).join("");
  }

  function validateReportRange(startDate, endDate) {
    const start = String(startDate || "").trim();
    const end = String(endDate || "").trim();
    if (!start || !end) {
      return "Start and End date are required.";
    }
    if (end < start) {
      return "End date must be on or after start date.";
    }
    const startObj = new Date(`${start}T00:00:00`);
    const endObj = new Date(`${end}T00:00:00`);
    if (Number.isNaN(startObj.getTime()) || Number.isNaN(endObj.getTime())) {
      return "Invalid date format.";
    }
    const days = Math.floor((endObj.getTime() - startObj.getTime()) / 86400000) + 1;
    if (days > 90) {
      return "Maximum report range is 90 days.";
    }
    return "";
  }

  async function loadReportTrades() {
    if (!hasReportUi() || reportBusy) {
      return;
    }
    const startDate = String(reportStartDateEl.value || "").trim();
    const endDate = String(reportEndDateEl.value || "").trim();
    const errMsg = validateReportRange(startDate, endDate);
    if (errMsg) {
      setText(reportStatusEl, errMsg);
      return;
    }
    const modeVal = reportModeSelectEl ? String(reportModeSelectEl.value || "all").trim() : "all";
    reportBusy = true;
    reportLoadBtn.disabled = true;
    setText(reportStatusEl, "Loading AI trade report...");
    try {
      const qs = `start_date=${encodeURIComponent(startDate)}&end_date=${encodeURIComponent(endDate)}&mode=${encodeURIComponent(modeVal)}`;
      const payload = await requestJson(`/user/report/ai-trades?${qs}`);
      const rows = Array.isArray(payload && payload.trades) ? payload.trades : [];
      renderReportRows(rows);
      setText(reportCountEl, `Trades: ${safeNum(payload && payload.count, rows.length)}`);
      const modeLabel = modeVal === "paper" ? "Paper" : modeVal === "live" ? "Live" : "Paper + Live";
      setText(reportStatusEl, `Loaded ${rows.length} ${modeLabel} trades for ${startDate} to ${endDate}.`);
    } catch (err) {
      renderReportRows([]);
      setText(reportCountEl, "Trades: 0");
      setText(reportStatusEl, `Report load failed: ${err.message}`);
    } finally {
      reportBusy = false;
      reportLoadBtn.disabled = false;
    }
  }

  function toLabel(value) {
    return String(value || "none").replaceAll("_", " ");
  }

  function toSentenceCase(value) {
    const text = String(value || "").trim();
    if (!text) {
      return "";
    }
    return text.charAt(0).toUpperCase() + text.slice(1);
  }

  function friendlyGroupName(groupRaw) {
    const group = String(groupRaw || "").toLowerCase();
    if (group === "call_buyers") {
      return "Call buyers";
    }
    if (group === "put_buyers") {
      return "Put buyers";
    }
    if (group === "call_sellers") {
      return "Call sellers";
    }
    if (group === "put_sellers") {
      return "Put sellers";
    }
    if (group === "buyers_both") {
      return "Both-side buyers";
    }
    return "No clear group";
  }

  function confidenceBand(confidence) {
    if (confidence >= 0.8) {
      return "high";
    }
    if (confidence >= 0.6) {
      return "moderate";
    }
    if (confidence >= 0.45) {
      return "building";
    }
    return "low";
  }

  function plainGuidance(guidanceRaw) {
    const guidance = String(guidanceRaw || "").toLowerCase();
    if (guidance === "wait") {
      return "Wait for a cleaner setup. Current move is not ready for execution.";
    }
    if (guidance === "observe") {
      return "Observe only. Let the structure form before taking a trade.";
    }
    if (guidance === "caution") {
      return "Trade only with strict risk control. Momentum exists but can fade quickly.";
    }
    if (guidance === "no_edge") {
      return "No clear edge right now. Preserve capital and avoid forcing entries.";
    }
    return "Follow your risk rules and wait for a clear pain-release setup.";
  }

  function simplePhaseText(phaseRaw) {
    const phase = String(phaseRaw || "").toLowerCase();
    if (phase === "comfort") {
      return "Market is calm and moving in a balanced way.";
    }
    if (phase === "late_entry") {
      return "Late traders are entering near the end of a move.";
    }
    if (phase === "discomfort") {
      return "Pressure is building and one side is getting uncomfortable.";
    }
    if (phase === "exit_pain") {
      return "A trapped side is exiting, so moves can become fast.";
    }
    if (phase === "exhaustion_pain") {
      return "The current move looks tired and can slow down.";
    }
    if (phase === "digestion") {
      return "Market is pausing and absorbing the previous move.";
    }
    if (phase === "transfer") {
      return "Control is shifting from one group to another.";
    }
    return "Market phase is updating.";
  }

  function phaseMeaning(phaseRaw) {
    const phase = String(phaseRaw || "").toLowerCase();
    if (phase === "comfort") {
      return "Market is in balance and pressure is limited.";
    }
    if (phase === "late_entry") {
      return "Late participants are entering near the end of a move.";
    }
    if (phase === "discomfort") {
      return "Pressure is building and one side is starting to feel trapped.";
    }
    if (phase === "exit_pain") {
      return "Forced exits are active, so displacement moves can accelerate.";
    }
    if (phase === "exhaustion_pain") {
      return "Pain move looks stretched; speed may start cooling.";
    }
    if (phase === "digestion") {
      return "Market is absorbing the last move and preparing a new direction.";
    }
    if (phase === "transfer") {
      return "Control is shifting from one participant group to another.";
    }
    return "Market phase is updating.";
  }

  function simplifyExplanation(rawExplanation) {
    const text = String(rawExplanation || "").trim();
    if (!text) {
      return "";
    }
    const flat = text
      .replaceAll("_", " ")
      .replaceAll("|", " ")
      .replace(/\s+/g, " ")
      .trim();
    return flat.length > 180 ? `${flat.slice(0, 180)}...` : flat;
  }

  function humanExplanation(snapshot) {
    const phase = simplePhaseText(snapshot.pain_phase);
    const dominant = friendlyGroupName(snapshot.dominant_pain_group || "none");
    const nextGroup = friendlyGroupName(snapshot.next_likely_pain_group || "none");
    const confidence = Math.max(0, Math.min(1, safeNum(snapshot.confidence, 0)));
    const confidencePct = Math.round(confidence * 100);
    const lines = [
      `Market view: ${phase}`,
      `Who is in control: ${dominant}.`,
      `Who can react next: ${nextGroup}.`,
      `Confidence: ${confidencePct}% (${confidenceBand(confidence)}).`,
      `What to do now: ${plainGuidance(snapshot.guidance)}`,
    ];
    return lines.join("\n");
  }

  function safeNum(value, fallback) {
    const num = Number(value);
    if (Number.isFinite(num)) {
      return num;
    }
    return fallback;
  }

  function formatTime(tsSeconds) {
    if (!tsSeconds) {
      return "No runtime timestamp yet";
    }
    const dt = new Date(Number(tsSeconds) * 1000);
    return `Runtime TS: ${dt.toLocaleString("en-IN", { timeZone: IST_TZ })}`;
  }

  function formatIst(tsSeconds) {
    if (!tsSeconds) {
      return "--";
    }
    const dt = new Date(Number(tsSeconds) * 1000);
    return dt.toLocaleString("en-IN", { timeZone: IST_TZ, hour12: false });
  }

  function toDateInput(tsSeconds) {
    const ts = safeNum(tsSeconds, 0);
    if (!(ts > 0)) {
      return "";
    }
    const dt = new Date(ts * 1000);
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: IST_TZ,
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
    }).formatToParts(dt);
    const map = {};
    parts.forEach((part) => {
      if (part.type !== "literal") {
        map[part.type] = part.value;
      }
    });
    return `${map.year || "1970"}-${map.month || "01"}-${map.day || "01"}`;
  }

  function chartTimeToSec(value) {
    if (typeof value === "number" && Number.isFinite(value)) {
      return Math.floor(value);
    }
    if (!value || typeof value !== "object") {
      return 0;
    }
    if (typeof value.timestamp === "number" && Number.isFinite(value.timestamp)) {
      return Math.floor(value.timestamp);
    }
    if (
      typeof value.year === "number"
      && typeof value.month === "number"
      && typeof value.day === "number"
    ) {
      return Math.floor(Date.UTC(value.year, value.month - 1, value.day) / 1000);
    }
    return 0;
  }

  function getIstTickParts(tsSeconds) {
    const dt = new Date(Number(tsSeconds) * 1000);
    const parts = IST_TICK_PARTS_FMT.formatToParts(dt);
    const out = { day: "", month: "", hour: "", minute: "", minuteNum: -1 };
    parts.forEach((part) => {
      if (part.type === "day") {
        out.day = part.value;
      } else if (part.type === "month") {
        out.month = part.value;
      } else if (part.type === "hour") {
        out.hour = part.value;
      } else if (part.type === "minute") {
        out.minute = part.value;
      }
    });
    out.minuteNum = Number(out.minute);
    return out;
  }

  function formatTickLabel(tsSeconds) {
    const parts = getIstTickParts(tsSeconds);
    const minuteNum = Number.isFinite(parts.minuteNum) ? parts.minuteNum : -1;
    if (minuteNum < 0) {
      return "";
    }
    const width = chartEl ? Math.max(0, chartEl.clientWidth) : 0;
    let stepMin = 5;
    if (chartTf === "1m") {
      if (width >= 1400) {
        stepMin = 1;
      } else if (width >= 1100) {
        stepMin = 2;
      } else if (width >= 850) {
        stepMin = 3;
      } else {
        stepMin = 5;
      }
    } else if (width >= 1400) {
      stepMin = 5;
    } else if (width >= 1100) {
      stepMin = 10;
    } else {
      stepMin = 15;
    }
    const isSessionOpenMark = parts.hour === "09" && parts.minute === "15";
    if (minuteNum % stepMin !== 0) {
      return "";
    }
    if (isSessionOpenMark) {
      return IST_TICK_DATE_TIME_FMT.format(new Date(Number(tsSeconds) * 1000));
    }
    return IST_TICK_TIME_FMT.format(new Date(Number(tsSeconds) * 1000));
  }

  function pulseCard(element) {
    if (!element) {
      return;
    }
    element.classList.remove("flash");
    void element.offsetWidth;
    element.classList.add("flash");
  }

  function parseJson(text) {
    if (!text) {
      return {};
    }
    try {
      return JSON.parse(text);
    } catch (_err) {
      return {};
    }
  }

  function setText(el, text) {
    if (!el) {
      return;
    }
    el.textContent = String(text || "");
  }

  function setPillState(el, state) {
    if (!el) {
      return;
    }
    el.classList.remove("is-good", "is-warn", "is-bad");
    if (state === "good") {
      el.classList.add("is-good");
    } else if (state === "warn") {
      el.classList.add("is-warn");
    } else if (state === "bad") {
      el.classList.add("is-bad");
    }
  }

  function formatReadyDuration(sec) {
    const total = Math.max(0, safeNum(sec, 0));
    const hrs = Math.floor(total / 3600);
    const mins = Math.floor((total % 3600) / 60);
    if (hrs > 0) {
      return `${hrs}h ${mins}m`;
    }
    return `${mins}m`;
  }

  function setTfButtons() {
    if (tf1mBtn) {
      tf1mBtn.classList.toggle("is-active", chartTf === "1m");
    }
    if (tf5mBtn) {
      tf5mBtn.classList.toggle("is-active", chartTf === "5m");
    }
  }

  function resizeChart() {
    if (!chart || !chartEl) {
      return;
    }
    chart.applyOptions({
      width: Math.max(260, chartEl.clientWidth),
      height: Math.max(260, chartEl.clientHeight),
    });
  }

  function scheduleChartStabilize() {
    if (!chart || !chartEl) {
      return;
    }
    const settle = function () {
      resizeChart();
      if (chartFitted && typeof chart.timeScale === "function") {
        chart.timeScale().scrollToRealTime();
      }
    };
    requestAnimationFrame(function () {
      settle();
      setTimeout(settle, 80);
      setTimeout(settle, 260);
      setTimeout(settle, 800);
    });
  }

  function addSeriesCompat(kind, options) {
    if (!chart) {
      return null;
    }
    if (kind === "candlestick" && typeof chart.addCandlestickSeries === "function") {
      return chart.addCandlestickSeries(options);
    }
    if (kind === "line" && typeof chart.addLineSeries === "function") {
      return chart.addLineSeries(options);
    }
    if (typeof chart.addSeries === "function" && window.LightweightCharts) {
      const typeObj = kind === "candlestick"
        ? window.LightweightCharts.CandlestickSeries
        : window.LightweightCharts.LineSeries;
      if (typeObj) {
        return chart.addSeries(typeObj, options);
      }
    }
    return null;
  }

  function initChart() {
    if (!chartEl || chart) {
      return;
    }
    if (!window.LightweightCharts || typeof window.LightweightCharts.createChart !== "function") {
      setText(chartStatusEl, "Chart library failed to load.");
      return;
    }

    chart = window.LightweightCharts.createChart(chartEl, {
      width: Math.max(260, chartEl.clientWidth),
      height: Math.max(260, chartEl.clientHeight),
      layout: {
        background: { type: "solid", color: "#fff9ed" },
        textColor: "#33434f",
        fontFamily: "IBM Plex Mono, monospace",
      },
      localization: {
        locale: "en-IN",
        timeFormatter: function (time) {
          const ts = chartTimeToSec(time);
          if (!(ts > 0)) {
            return "";
          }
          return IST_TIME_FMT.format(new Date(ts * 1000));
        },
      },
      grid: {
        vertLines: { color: "#eadcc1" },
        horzLines: { color: "#eadcc1" },
      },
      rightPriceScale: {
        borderColor: "#ccb58c",
      },
      timeScale: {
        borderColor: "#ccb58c",
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 6,
        barSpacing: 12,
        minBarSpacing: 2.2,
        tickMarkFormatter: function (time) {
          const ts = chartTimeToSec(time);
          if (!(ts > 0)) {
            return "";
          }
          return formatTickLabel(ts);
        },
      },
      crosshair: {
        mode: 1,
      },
    });

    candleSeries = addSeriesCompat("candlestick", {
      upColor: "#2c8550",
      downColor: "#b44a38",
      borderVisible: false,
      wickUpColor: "#2c8550",
      wickDownColor: "#b44a38",
    });
    tickSeries = addSeriesCompat("line", {
      color: "#256f86",
      lineWidth: 1,
      priceLineVisible: false,
      lastValueVisible: false,
      crosshairMarkerVisible: true,
    });
    if (!candleSeries || !tickSeries) {
      setText(chartStatusEl, "Chart API mismatch. Refresh page once.");
      return;
    }

    if ("ResizeObserver" in window) {
      resizeObserver = new ResizeObserver(resizeChart);
      resizeObserver.observe(chartEl);
    } else {
      window.addEventListener("resize", resizeChart);
    }
    if (!chartAutoSized) {
      chartAutoSized = true;
      scheduleChartStabilize();
      window.addEventListener("load", scheduleChartStabilize, { passive: true });
      document.addEventListener("visibilitychange", function () {
        if (!document.hidden) {
          scheduleChartStabilize();
        }
      });
      if (document.fonts && typeof document.fonts.ready === "object") {
        document.fonts.ready.then(scheduleChartStabilize).catch(function () {});
      }
    }
  }

  function toChartCandles(rows) {
    if (!Array.isArray(rows)) {
      return [];
    }
    return rows
      .map((row) => {
        const time = Math.floor(safeNum(row.timestamp, 0));
        const open = safeNum(row.open, NaN);
        const high = safeNum(row.high, NaN);
        const low = safeNum(row.low, NaN);
        const close = safeNum(row.close, NaN);
        if (!(time > 0)) {
          return null;
        }
        if (![open, high, low, close].every(Number.isFinite)) {
          return null;
        }
        if (open <= 0 || high <= 0 || low <= 0 || close <= 0) {
          return null;
        }
        const normalizedHigh = Math.max(high, open, close, low);
        const normalizedLow = Math.min(low, open, close, high);
        return {
          time: time,
          open: open,
          high: normalizedHigh,
          low: normalizedLow,
          close: close,
        };
      })
      .filter(Boolean)
      .sort((a, b) => a.time - b.time);
  }

  function updateTickTrail(tick) {
    const ts = Math.floor(safeNum(tick && tick.timestamp, 0));
    const price = safeNum(tick && tick.price, NaN);
    if (!(ts > 0) || !Number.isFinite(price)) {
      return;
    }
    if (tickTrail.length && tickTrail[tickTrail.length - 1].time === ts) {
      tickTrail[tickTrail.length - 1].value = price;
      return;
    }
    tickTrail.push({ time: ts, value: price });
    if (tickTrail.length > 1800) {
      tickTrail = tickTrail.slice(-1800);
    }
  }

  function focusRecentCandles(candles) {
    if (!chart || !candles || !candles.length) {
      return;
    }
    const last = candles[candles.length - 1];
    const stepSec = chartTf === "5m" ? 300 : 60;
    // Keep a tighter viewport so candles are clearly visible after refresh.
    const visibleBars = chartTf === "5m" ? 40 : 24;
    const from = Math.max(1, last.time - (visibleBars * stepSec));
    const to = last.time + (stepSec * 5);
    try {
      chart.timeScale().setVisibleRange({ from: from, to: to });
    } catch (_err) {
      chart.timeScale().fitContent();
    }
    const tailCount = chartTf === "5m" ? 40 : 24;
    const tail = candles.slice(-Math.min(candles.length, tailCount));
    let low = Number.POSITIVE_INFINITY;
    let high = Number.NEGATIVE_INFINITY;
    tail.forEach((bar) => {
      const lo = safeNum(bar.low, NaN);
      const hi = safeNum(bar.high, NaN);
      if (Number.isFinite(lo) && lo < low) {
        low = lo;
      }
      if (Number.isFinite(hi) && hi > high) {
        high = hi;
      }
    });
    const lastClose = safeNum(last.close, NaN);
    if (Number.isFinite(low) && Number.isFinite(high) && Number.isFinite(lastClose)) {
      // Some feeds may create flat OHLC bars (high === low). Keep the chart readable anyway.
      if (!(high > low)) {
        const basePad = Math.max(Math.abs(lastClose) * 0.0015, chartTf === "5m" ? 2.5 : 1.25);
        low = lastClose - basePad;
        high = lastClose + basePad;
      }
      const spread = Math.max(high - low, Math.abs(lastClose) * 0.0008);
      const pad = Math.max(spread * 0.18, Math.abs(lastClose) * 0.0012);
      try {
        const rightScale = chart.priceScale("right");
        if (rightScale && typeof rightScale.setVisibleRange === "function") {
          rightScale.setVisibleRange({ from: low - pad, to: high + pad });
        }
      } catch (_err) {
        // Keep autoscale fallback if custom range is not available in current chart API.
      }
    }
  }

  function renderMarketChart(payload) {
    lastMarket = payload || null;
    initChart();
    if (!chart || !candleSeries || !tickSeries || !payload) {
      return;
    }

    const source = chartTf === "5m" ? payload.candles_5m : payload.candles_1m;
    const candles = toChartCandles(source);

    if (chartTf === "1m" && payload.current_1m && safeNum(payload.current_1m.timestamp, 0) > 0) {
      const live = toChartCandles([payload.current_1m])[0];
      if (live) {
        const last = candles.length ? candles[candles.length - 1] : null;
        if (last && last.time === live.time) {
          candles[candles.length - 1] = live;
        } else {
          candles.push(live);
        }
      }
    }

    if (!chartHistoryLoaded) {
      candleSeries.setData(candles);
      chartLastBarTime = candles.length ? candles[candles.length - 1].time : 0;
      tickTrail = [];
      lastTickTime = 0;
      chartHistoryLoaded = candles.length > 0;
      if (candles.length > 0) {
        focusRecentCandles(candles);
        chartFitted = true;
      }
    } else if (candles.length) {
      if (chartLastBarTime <= 0) {
        candleSeries.setData(candles);
        chartLastBarTime = candles[candles.length - 1].time;
        focusRecentCandles(candles);
        chartFitted = true;
      } else {
      // Incremental updates feel smoother than replacing full series every poll.
      // Update only same-time or newer bars; older bars cause chart library errors.
        const incremental = candles
          .filter((bar) => bar.time >= chartLastBarTime)
          .sort((a, b) => a.time - b.time);
        try {
          if (incremental.length) {
            incremental.forEach((bar) => {
              candleSeries.update(bar);
              if (bar.time > chartLastBarTime) {
                chartLastBarTime = bar.time;
              }
            });
          }
        } catch (_err) {
          // If any out-of-order payload slips through, recover by replacing series.
          candleSeries.setData(candles);
          chartLastBarTime = candles.length ? candles[candles.length - 1].time : chartLastBarTime;
          if (candles.length > 0) {
            focusRecentCandles(candles);
          }
        }
      }
    }

    if (payload.last_tick) {
      updateTickTrail(payload.last_tick);
      const tail = tickTrail.length ? tickTrail[tickTrail.length - 1] : null;
      if (tail) {
        if (lastTickTime === 0) {
          tickSeries.setData([tail]);
          lastTickTime = tail.time;
        } else if (tail.time >= lastTickTime) {
          tickSeries.update(tail);
          lastTickTime = tail.time;
        }
      }
    }

    if (!chartFitted && candles.length > 0) {
      focusRecentCandles(candles);
      chartFitted = true;
      scheduleChartStabilize();
    }

    const tfTitle = chartTf === "5m" ? "5m analysis" : "1m execution";
    const tickCount = tickTrail.length;
    setText(
      chartStatusEl,
      `Chart: ${tfTitle} | candles ${candles.length} | tick ${tickCount ? "live" : "waiting"} | time: IST`,
    );
  }

  function onTfChange(nextTf) {
    chartTf = nextTf === "5m" ? "5m" : "1m";
    setTfButtons();
    chartHistoryLoaded = false;
    chartLastBarTime = 0;
    lastTickTime = 0;
    chartFitted = false;
    tickTrail = [];
    if (candleSeries && typeof candleSeries.setData === "function") {
      candleSeries.setData([]);
    }
    if (tickSeries && typeof tickSeries.setData === "function") {
      tickSeries.setData([]);
    }
    setText(chartStatusEl, `Loading ${chartTf} candles from database...`);
    refreshMarketChart(true);
  }

  async function requestJson(path, options) {
    const opts = options || {};
    const method = opts.method || "GET";
    const headers = Object.assign({}, opts.headers || {});
    const config = {
      method: method,
      headers: headers,
      cache: "no-store",
      credentials: "same-origin",
    };
    if (opts.data !== undefined) {
      headers["content-type"] = "application/json";
      config.body = JSON.stringify(opts.data);
    }

    const res = await fetch(path, config);
    const text = await res.text();
    const body = parseJson(text);
    if (!res.ok) {
      const detail = body.detail || body.message || `HTTP ${res.status} on ${path}`;
      throw new Error(detail);
    }
    return body;
  }

  function setUserLine(user) {
    const primary = user ? String(user.email || user.username || "trader") : "trader";
    const fullName = user && user.full_name ? String(user.full_name) : "Sensible Algo User";
    const mobile = user && (user.mobile_number || user.mobile) ? String(user.mobile_number || user.mobile) : "--";
    const plan = user && user.plan_name ? String(user.plan_name) : "Starter";
    userLotCount = Math.max(1, safeNum(user && user.user_lot_count, userLotCount || 1));
    setSubscriptionStatusFromData(user);
    if (userLineEl) {
      userLineEl.textContent = `Signed in as ${primary}${fullName ? ` (${fullName})` : ""}`;
    }
    setText(profileNameEl, fullName);
    setText(profileEmailEl, primary);
    setText(profileMobileEl, mobile);
    setText(profilePlanEl, plan);
    if (profileLotCountEl) {
      setText(profileLotCountEl, String(Math.trunc(userLotCount)));
    }
    if (userLotCountInputEl) {
      userLotCountInputEl.value = String(Math.trunc(userLotCount));
    }
    if (profileAvatarTextEl) {
      const seed = fullName && fullName !== "Sensible Algo User" ? fullName : primary;
      profileAvatarTextEl.textContent = String(seed || "U").trim().charAt(0).toUpperCase() || "U";
    }
  }

  async function refreshBrokerClientId() {
    if (!hasBrokerUi() || brokerBusy) {
      return;
    }
    brokerBusy = true;
    try {
      const payload = await requestJson("/user/broker/angel-one");
      const config = payload && typeof payload.config === "object" ? payload.config : {};
      const clientId = String(config.client_id || "").trim();
      const apiKeyHint = String(config.api_key_hint || "").trim();
      const apiKeySaved = Boolean(config.api_key_saved);
      const pinSaved = Boolean(config.pin_saved);
      const totpSaved = Boolean(config.totp_secret_saved);
      const hasLoginCredentials = Boolean(config.has_login_credentials);
      const connected = Boolean(config.connected);
      const engineEnabled = Boolean(config.trading_engine_enabled || config.enabled);
      setSubscriptionStatusFromData(config);
      setLotConfigFromData({ user_lot_count: config.user_lot_count });
      setText(brokerClientIdValueEl, clientId || "Not set");
      setText(brokerApiKeyValueEl, apiKeySaved ? (apiKeyHint || "Saved") : "Not saved");
      setText(brokerPinValueEl, pinSaved ? "Saved" : "Not saved");
      setText(brokerTotpValueEl, totpSaved ? "Saved" : "Not saved");
      if (brokerClientIdInputEl) {
        brokerClientIdInputEl.value = clientId;
      }
      if (brokerApiKeyInputEl) {
        brokerApiKeyInputEl.value = "";
      }
      if (brokerPinInputEl) {
        brokerPinInputEl.value = "";
      }
      if (brokerTotpInputEl) {
        brokerTotpInputEl.value = "";
      }
      setTerminalLoginConnected(connected);
      setTradingEngineEnabled(engineEnabled);
      await refreshPaper();
      setText(
        brokerClientIdMsgEl,
        connected
          ? "Angel One connected."
          : (hasLoginCredentials ? "Angel credentials loaded." : (clientId ? "Client ID loaded. Save API key, PIN, and TOTP secret to enable terminal login." : "No Angel credentials saved yet.")),
      );
    } catch (err) {
      setText(brokerClientIdMsgEl, `Load failed: ${err.message}`);
      setTerminalLoginConnected(false);
    } finally {
      brokerBusy = false;
    }
  }

  async function refreshUserLotConfig() {
    if (!hasUserLotUi() || lotConfigBusy) {
      return;
    }
    lotConfigBusy = true;
    try {
      const payload = await requestJson("/user/lots");
      const config = payload && typeof payload.config === "object" ? payload.config : {};
      setLotConfigFromData(config);
      if (userLotCountMsgEl) {
        setText(
          userLotCountMsgEl,
          `Saved lots: ${Math.trunc(userLotCount)} | Lot size: ${Math.trunc(lotSizeQty)} | Order qty: ${Math.trunc(getUserQuantityMultiplier())}`,
        );
      }
    } catch (err) {
      if (userLotCountMsgEl) {
        setText(userLotCountMsgEl, `Lot settings load failed: ${err.message}`);
      }
    } finally {
      lotConfigBusy = false;
    }
  }

  async function onUserLotCountSave() {
    if (!userLotCountInputEl || !userLotCountSaveBtn) {
      return;
    }
    const requested = Math.trunc(safeNum(userLotCountInputEl.value, 0));
    if (requested <= 0) {
      if (userLotCountMsgEl) {
        setText(userLotCountMsgEl, "Lot count must be at least 1.");
      }
      return;
    }
    userLotCountSaveBtn.disabled = true;
    if (userLotCountMsgEl) {
      setText(userLotCountMsgEl, "Saving lot count...");
    }
    try {
      const payload = await requestJson("/user/lots", {
        method: "POST",
        data: { lot_count: requested },
      });
      const config = payload && typeof payload.config === "object" ? payload.config : {};
      setLotConfigFromData(config);
      if (userLotCountMsgEl) {
        setText(
          userLotCountMsgEl,
          `Lot count saved. Effective order quantity: ${Math.trunc(getUserQuantityMultiplier())}`,
        );
      }
      await refreshPaper();
      if (hasReportUi()) {
        await loadReportTrades();
      }
    } catch (err) {
      if (userLotCountMsgEl) {
        setText(userLotCountMsgEl, `Save failed: ${err.message}`);
      }
    } finally {
      userLotCountSaveBtn.disabled = false;
    }
  }

  async function refreshAdminLotSize() {
    if (!hasAdminLotUi()) {
      return;
    }
    try {
      const payload = await requestJson("/admin/execution/lot-size");
      const lot = Math.max(1, Math.trunc(safeNum(payload && payload.lot_size_qty, 1)));
      lotSizeQty = lot;
      if (adminLotSizeInputEl) {
        adminLotSizeInputEl.value = String(lot);
      }
      if (adminLotSizeCurrentEl) {
        setText(adminLotSizeCurrentEl, `Current lot size: ${lot}`);
      }
      if (adminLotSizeMsgEl) {
        setText(adminLotSizeMsgEl, "Lot size loaded.");
      }
    } catch (err) {
      if (adminLotSizeMsgEl) {
        setText(adminLotSizeMsgEl, `Load failed: ${err.message}`);
      }
    }
  }

  async function onAdminLotSizeSave() {
    if (!adminLotSizeInputEl || !adminLotSizeSaveBtn) {
      return;
    }
    const requested = Math.trunc(safeNum(adminLotSizeInputEl.value, 0));
    if (requested <= 0) {
      if (adminLotSizeMsgEl) {
        setText(adminLotSizeMsgEl, "Lot size must be at least 1.");
      }
      return;
    }
    adminLotSizeSaveBtn.disabled = true;
    if (adminLotSizeMsgEl) {
      setText(adminLotSizeMsgEl, "Saving lot size...");
    }
    try {
      const payload = await requestJson("/admin/execution/lot-size", {
        method: "POST",
        data: { lot_size_qty: requested },
      });
      const lot = Math.max(1, Math.trunc(safeNum(payload && payload.lot_size_qty, requested)));
      lotSizeQty = lot;
      if (adminLotSizeInputEl) {
        adminLotSizeInputEl.value = String(lot);
      }
      if (adminLotSizeCurrentEl) {
        setText(adminLotSizeCurrentEl, `Current lot size: ${lot}`);
      }
      if (adminLotSizeMsgEl) {
        setText(adminLotSizeMsgEl, "Lot size saved.");
      }
    } catch (err) {
      if (adminLotSizeMsgEl) {
        setText(adminLotSizeMsgEl, `Save failed: ${err.message}`);
      }
    } finally {
      adminLotSizeSaveBtn.disabled = false;
    }
  }

  async function onTradingEngineToggle() {
    if (!tradingEngineToggleEl || tradingEngineToggleBusy) {
      return;
    }
    tradingEngineToggleBusy = true;
    tradingEngineToggleEl.disabled = true;
    if (tradingEngineMsgEl) {
      setText(tradingEngineMsgEl, "Updating trading engine...");
    }
    try {
      const payload = await requestJson("/user/trading-engine", {
        method: "POST",
        data: { enabled: !tradingEngineEnabled },
      });
      const config = payload && typeof payload.config === "object" ? payload.config : {};
      setSubscriptionStatusFromData(config);
      const savedEnabled = Boolean(config.enabled);
      setTradingEngineEnabled(savedEnabled);
      await refreshPaper();
    } catch (err) {
      if (tradingEngineMsgEl) {
        setText(tradingEngineMsgEl, `Trading engine update failed: ${err.message}`);
      }
    } finally {
      tradingEngineToggleEl.disabled = !subscriptionPaidActive;
      tradingEngineToggleBusy = false;
    }
  }

  async function onSubscribeNow(planCodeOverride) {
    if (subscribeBusy) {
      return;
    }
    const planCode = normalizePlanCode(planCodeOverride || getSelectedPlanCode());
    updateSubscribePlanUi(planCode);
    if (typeof window.Razorpay !== "function") {
      setText(subscribeMsgEl, "Razorpay checkout failed to load. Refresh and try again.");
      return;
    }
    subscribeBusy = true;
    setSubscribeBusyState(true);
    setText(subscribeMsgEl, "Creating payment order...");
    try {
      const localPlan = SUBSCRIPTION_PLANS[planCode] || SUBSCRIPTION_PLANS.monthly;
      const order = await requestJson("/user/subscription/razorpay/order", {
        method: "POST",
        data: { plan_code: planCode },
      });
      const key = String(order.key_id || "").trim();
      const orderId = String(order.order_id || "").trim();
      if (!key || !orderId) {
        throw new Error("Invalid payment order");
      }
      const prefill = (order && order.prefill && typeof order.prefill === "object") ? order.prefill : {};
      const localUser = currentUser && typeof currentUser === "object" ? currentUser : {};
      const finalPrefill = {
        name: String(prefill.name || localUser.full_name || "").trim(),
        email: String(prefill.email || localUser.email || "").trim(),
        contact: String(prefill.contact || localUser.mobile_number || "").trim(),
      };

      const options = {
        key: key,
        amount: Number(order.amount || 0),
        currency: String(order.currency || "INR"),
        name: "Sensible Algo",
        description: String(order.plan_name || `${localPlan.label} Subscription`),
        order_id: orderId,
        prefill: finalPrefill,
        theme: { color: "#1f4ed8" },
        handler: async function (response) {
          try {
            const verify = await requestJson("/user/subscription/razorpay/verify", {
              method: "POST",
              data: {
                razorpay_order_id: String(response.razorpay_order_id || ""),
                razorpay_payment_id: String(response.razorpay_payment_id || ""),
                razorpay_signature: String(response.razorpay_signature || ""),
                plan_name: String(order.plan_name || "Sensible Algo Pro"),
                plan_code: planCode,
              },
            });
            const planName = String((verify && verify.plan_name) || order.plan_name || "Sensible Algo Pro");
            const subObj = verify && verify.subscription && typeof verify.subscription === "object"
              ? verify.subscription
              : { subscription_active: true, subscription_plan_name: planName };
            setSubscriptionStatusFromData(subObj);
            setText(profilePlanEl, subscriptionPlanName || planName);
            setTradingEngineEnabled(tradingEngineEnabled);
            setText(subscribeMsgEl, `Payment successful. Plan activated: ${planName}`);
          } catch (err) {
            setText(subscribeMsgEl, `Payment captured but verification failed: ${err.message}`);
          }
        },
        modal: {
          ondismiss: function () {
            setText(subscribeMsgEl, "Payment cancelled.");
          },
        },
      };
      const razorpay = new window.Razorpay(options);
      razorpay.open();
      setText(subscribeMsgEl, "Complete payment in Razorpay window.");
    } catch (err) {
      setText(subscribeMsgEl, `Subscription failed: ${err.message}`);
    } finally {
      subscribeBusy = false;
      setSubscribeBusyState(false);
    }
  }

  async function onBrokerClientIdSave() {
    if (!hasBrokerUi() || !brokerClientIdInputEl) {
      return;
    }
    const clientId = String(brokerClientIdInputEl.value || "").trim();
    const apiKey = brokerApiKeyInputEl ? String(brokerApiKeyInputEl.value || "").trim() : "";
    const pin = brokerPinInputEl ? String(brokerPinInputEl.value || "").trim() : "";
    const totpSecret = brokerTotpInputEl ? String(brokerTotpInputEl.value || "").trim() : "";
    if (!clientId) {
      setText(brokerClientIdMsgEl, "Client ID is required.");
      return;
    }
    if (brokerClientIdSaveBtn) {
      brokerClientIdSaveBtn.disabled = true;
    }
    setText(brokerClientIdMsgEl, "Saving...");
    try {
      const payload = await requestJson("/user/broker/angel-one", {
        method: "POST",
        data: {
          client_id: clientId,
          api_key: apiKey,
          pin: pin,
          totp_secret: totpSecret,
        },
      });
      const config = payload && typeof payload.config === "object" ? payload.config : {};
      const saved = String(config.client_id || "").trim();
      setText(brokerClientIdValueEl, saved || "Not set");
      setText(brokerApiKeyValueEl, config.api_key_saved ? String(config.api_key_hint || "Saved") : "Not saved");
      setText(brokerPinValueEl, config.pin_saved ? "Saved" : "Not saved");
      setText(brokerTotpValueEl, config.totp_secret_saved ? "Saved" : "Not saved");
      if (brokerClientIdInputEl) {
        brokerClientIdInputEl.value = saved;
      }
      if (brokerApiKeyInputEl) {
        brokerApiKeyInputEl.value = "";
      }
      if (brokerPinInputEl) {
        brokerPinInputEl.value = "";
      }
      if (brokerTotpInputEl) {
        brokerTotpInputEl.value = "";
      }
      setTerminalLoginConnected(Boolean(config.connected));
      setText(brokerClientIdMsgEl, "Angel credentials saved.");
    } catch (err) {
      setText(brokerClientIdMsgEl, `Save failed: ${err.message}`);
    } finally {
      if (brokerClientIdSaveBtn) {
        brokerClientIdSaveBtn.disabled = false;
      }
    }
  }

  async function onTerminalLoginToggle() {
    if (!terminalLoginToggleEl) {
      return;
    }
    if (!hasBrokerUi()) {
      return;
    }
    if (terminalToggleBusy) {
      return;
    }
    terminalToggleBusy = true;
    terminalLoginToggleEl.disabled = true;
    try {
      if (terminalConnected) {
        setText(brokerClientIdMsgEl, "Disconnecting Angel One terminal...");
        const payload = await requestJson("/user/broker/angel-one/disconnect", {
          method: "POST",
        });
        const config = payload && typeof payload.config === "object" ? payload.config : {};
        setTerminalLoginConnected(Boolean(config.connected));
        setTradingEngineEnabled(tradingEngineEnabled);
        setText(brokerClientIdMsgEl, "Terminal login switched off.");
        return;
      }
      const clientId = brokerClientIdInputEl ? String(brokerClientIdInputEl.value || "").trim() : "";
      if (!clientId) {
        setText(brokerClientIdMsgEl, "Set and save client ID first.");
        return;
      }
      setText(brokerClientIdMsgEl, "Logging in to Angel terminal...");
      const payload = await requestJson("/user/broker/angel-one/login", {
        method: "POST",
      });
      const config = payload && typeof payload.config === "object" ? payload.config : {};
      setTerminalLoginConnected(Boolean(config.connected));
      setTradingEngineEnabled(tradingEngineEnabled);
      setText(brokerClientIdMsgEl, "Angel One terminal connected.");
    } catch (err) {
      setText(brokerClientIdMsgEl, `Terminal login failed: ${err.message}`);
    } finally {
      terminalLoginToggleEl.disabled = false;
      terminalToggleBusy = false;
    }
  }

  function applyAngelCallbackMessage() {
    if (!hasBrokerUi()) {
      return;
    }
    const params = new URLSearchParams(window.location.search || "");
    const angel = String(params.get("angel") || "").trim().toLowerCase();
    if (!angel) {
      return;
    }
    const reason = String(params.get("reason") || "").trim();
    if (angel === "connected") {
      setText(brokerClientIdMsgEl, "Angel One terminal connected.");
    } else if (angel === "failed") {
      setText(
        brokerClientIdMsgEl,
        reason ? `Angel login failed: ${reason}` : "Angel login failed.",
      );
    }
    params.delete("angel");
    params.delete("reason");
    params.delete("clientId");
    const cleanQuery = params.toString();
    const cleanUrl = `${window.location.pathname}${cleanQuery ? `?${cleanQuery}` : ""}${window.location.hash || ""}`;
    window.history.replaceState({}, document.title, cleanUrl);
  }

  function pushTimeline(snapshot) {
    if (!snapshot) {
      return;
    }
    const now = new Date().toLocaleTimeString("en-IN", { hour12: false });
    const item = `${now} | ${snapshot.pain_phase} | ${snapshot.dominant_pain_group} -> ${snapshot.next_likely_pain_group}`;
    if (!timeline.length || timeline[0] !== item) {
      timeline.unshift(item);
      timeline = timeline.slice(0, 12);
    }
    timelineEl.innerHTML = timeline.map((row) => `<li>${row}</li>`).join("");
  }

  function renderFeatures(rawState) {
    const features = rawState && rawState.features ? rawState.features : {};
    const rows = Object.entries(features)
      .map(([key, value]) => [key, safeNum(value, 0)])
      .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))
      .slice(0, 8);

    if (!rows.length) {
      featureListEl.innerHTML = '<p class="small">No feature vector yet. Feed at least 30 candles.</p>';
      return;
    }

    const maxAbs = Math.max(...rows.map((row) => Math.abs(row[1])), 1);
    featureListEl.innerHTML = rows.map(([name, value]) => {
      const pct = Math.max(2, Math.round((Math.abs(value) / maxAbs) * 100));
      return `
        <div class="feature">
          <div class="feature-name">
            <span>${toLabel(name)}</span>
            <span class="mono">${value.toFixed(3)}</span>
          </div>
          <div class="feature-track"><div class="feature-fill" style="width:${pct}%"></div></div>
        </div>
      `;
    }).join("");
  }

  function applySnapshot(rawState, state) {
    const snapshot = Object.assign({}, state || {}, rawState || {});
    setText(painPhaseEl, toLabel(snapshot.pain_phase));
    setText(dominantGroupEl, toLabel(snapshot.dominant_pain_group));
    setText(nextGroupEl, toLabel(snapshot.next_likely_pain_group));
    setText(guidanceEl, toLabel(snapshot.guidance));
    setText(explanationEl, humanExplanation(snapshot));

    const confidence = Math.max(0, Math.min(1, safeNum(snapshot.confidence, 0)));
    const pct = Math.round(confidence * 100);
    setText(confidenceTextEl, `${pct}%`);
    if (confidenceFillEl) {
      confidenceFillEl.style.width = `${pct}%`;
    }
    setText(lastUpdateEl, formatTime(snapshot.timestamp));

    if (painPhaseEl) {
      painPhaseEl.className = "";
      painPhaseEl.classList.add(`phase-${String(snapshot.pain_phase || "").toLowerCase()}`);
    }

    const changed = !lastSnapshot
      || lastSnapshot.pain_phase !== snapshot.pain_phase
      || lastSnapshot.dominant_pain_group !== snapshot.dominant_pain_group
      || lastSnapshot.next_likely_pain_group !== snapshot.next_likely_pain_group;
    if (changed) {
      if (painPhaseEl) {
        const cardEl = painPhaseEl.closest(".card");
        pulseCard(cardEl);
      }
      pushTimeline(snapshot);
    }
    lastSnapshot = snapshot;
    renderFeatures(rawState);
  }

  function homeOptionLabelFromSignal(signalObj) {
    const signal = signalObj && typeof signalObj === "object" ? signalObj : {};
    const selected = signal.selected_option && typeof signal.selected_option === "object"
      ? signal.selected_option
      : null;
    if (!selected) {
      return "--";
    }
    const sym = String(selected.symbol || selected.tradingsymbol || "").trim();
    if (sym) {
      return sym;
    }
    const side = String(selected.side || "").toUpperCase();
    const strike = safeNum(selected.strike, NaN);
    const optType = String(selected.option_type || selected.type || "").toUpperCase();
    if (!Number.isFinite(strike)) {
      return side || "--";
    }
    const strikeText = strike % 1 === 0 ? strike.toFixed(0) : strike.toFixed(2);
    return optType ? `${strikeText}${optType}` : `${side} ${strikeText}`;
  }

  function toTradeIdKey(value) {
    const num = Number(value);
    if (Number.isFinite(num)) {
      return String(Math.trunc(num));
    }
    return String(value || "").trim();
  }

  function renderPaperState(paper) {
    if (!paper || typeof paper !== "object") {
      return;
    }
    if (paperBackendEl) {
      paperBackendEl.textContent = `Backend: ${String(paper.storage_backend || "unknown")}`;
    }
    paperClosedEl.textContent = String(safeNum(paper.closed_trades, 0));
    paperProfitEl.textContent = String(safeNum(paper.profit_trades, 0));
    paperLossEl.textContent = String(safeNum(paper.loss_trades, 0));
    paperWinRateEl.textContent = `${safeNum(paper.win_rate_pct, 0).toFixed(2)}%`;
    paperNetEl.textContent = safeNum(paper.net_points, 0).toFixed(2);

    const active = paper.active_trade || null;
    const qtyMultiplier = getUserQuantityMultiplier();
    if (active) {
      const side = String(active.direction || "").toUpperCase();
      const entry = formatIst(active.entry_ts);
      const source = String(active.pnl_source || "option_quote_pending");
      const markLabel = source.indexOf("option_") === 0 ? "Option LTP" : "Mark";
      const mark = safeNum(active.mark_price, 0).toFixed(2);
      const upnl = (safeNum(active.unrealized_points, 0) * qtyMultiplier).toFixed(2);
      const contract = homeOptionLabelFromSignal(active.signal);
      if (source === "option_quote_pending") {
        paperActiveTradeEl.textContent = `Active: ${side} | ${contract} | Entry ${entry} | Awaiting option quote...`;
      } else {
        paperActiveTradeEl.textContent = `Active: ${side} | ${contract} | Entry ${entry} | ${markLabel} ${mark} | uPnL ${upnl}`;
      }
    } else {
      paperActiveTradeEl.textContent = "No active paper trade.";
    }
    renderPaperDiagnostics(paper.gate_diagnostics);

    const todayIst = formatTodayIstDate();
    const userConsoleOnlyToday = document.body.classList.contains("user-console");
    const recentAll = Array.isArray(paper.recent_trades) ? paper.recent_trades.slice().reverse() : [];
    const recent = userConsoleOnlyToday
      ? recentAll.filter(function (row) {
        return toDateInput(safeNum(row && row.entry_ts, 0)) === todayIst;
      })
      : recentAll;
    if (userConsoleOnlyToday) {
      const closed = recent.length;
      let profit = 0;
      let loss = 0;
      let net = 0;
      recent.forEach(function (row) {
        const points = safeNum(row && row.points, 0) * qtyMultiplier;
        net += points;
        if (points > 0) {
          profit += 1;
        } else if (points < 0) {
          loss += 1;
        }
      });
      const winRate = closed > 0 ? (profit * 100) / closed : 0;
      paperClosedEl.textContent = String(closed);
      paperProfitEl.textContent = String(profit);
      paperLossEl.textContent = String(loss);
      paperWinRateEl.textContent = `${safeNum(winRate, 0).toFixed(2)}%`;
      paperNetEl.textContent = safeNum(net, 0).toFixed(2);
    }
    if (!recent.length) {
      paperTradesBodyEl.innerHTML = userConsoleOnlyToday
        ? '<tr><td colspan="7" class="small">No paper trades for today.</td></tr>'
        : '<tr><td colspan="7" class="small">No paper trades yet.</td></tr>';
      return;
    }
    paperTradesBodyEl.innerHTML = recent.map((row) => {
      const id = safeNum(row.trade_id, 0);
      const direction = String(row.direction || "--");
      const contract = homeOptionLabelFromSignal(row.signal);
      const entry = formatIst(row.entry_ts);
      const exit = formatIst(row.exit_ts);
      const points = (safeNum(row.points, 0) * qtyMultiplier).toFixed(2);
      const outcome = String(row.outcome || "flat");
      return `<tr>
        <td>${id}</td>
        <td>${direction}</td>
        <td>${contract}</td>
        <td>${entry}</td>
        <td>${exit}</td>
        <td>${points}</td>
        <td>${outcome}</td>
      </tr>`;
    }).join("");
  }

  function isHomeLiveMode() {
    return document.body.classList.contains("user-console") && Boolean(tradingEngineEnabled);
  }

  function renderLiveHomeState(livePayload, paperPayload) {
    const paper = paperPayload && typeof paperPayload === "object" ? paperPayload : {};
    const qtyMultiplier = getUserQuantityMultiplier();
    const rowsRaw = livePayload && Array.isArray(livePayload.trades) ? livePayload.trades : [];
    const rows = rowsRaw.slice().sort(function (a, b) {
      return safeNum(b && b.entry_ts, 0) - safeNum(a && a.entry_ts, 0);
    });
    const activeRow = rows.find(function (row) {
      const status = String(row && row.status || "").toLowerCase();
      const outcome = String(row && row.outcome || "").toLowerCase();
      return status === "open" || outcome === "open";
    }) || null;
    const closedRows = rows.filter(function (row) {
      const status = String(row && row.status || "").toLowerCase();
      const outcome = String(row && row.outcome || "").toLowerCase();
      return !(status === "open" || outcome === "open");
    });
    const strategyRows = Array.isArray(paper.recent_trades) ? paper.recent_trades : [];
    const strategyByTradeId = {};
    strategyRows.forEach(function (row) {
      const key = toTradeIdKey(row && row.trade_id);
      if (key) {
        strategyByTradeId[key] = row;
      }
    });

    let resolvedCount = 0;
    let profit = 0;
    let loss = 0;
    let net = 0;
    closedRows.forEach(function (row) {
      const key = toTradeIdKey(row && row.trade_id);
      const linked = strategyByTradeId[key];
      const points = safeNum(linked && linked.points, NaN) * qtyMultiplier;
      if (!Number.isFinite(points)) {
        return;
      }
      resolvedCount += 1;
      net += points;
      if (points > 0) {
        profit += 1;
      } else if (points < 0) {
        loss += 1;
      }
    });

    paperClosedEl.textContent = String(closedRows.length);
    if (resolvedCount > 0) {
      const winRate = (profit * 100) / resolvedCount;
      paperProfitEl.textContent = String(profit);
      paperLossEl.textContent = String(loss);
      paperWinRateEl.textContent = `${safeNum(winRate, 0).toFixed(2)}%`;
      paperNetEl.textContent = safeNum(net, 0).toFixed(2);
    } else {
      paperProfitEl.textContent = "--";
      paperLossEl.textContent = "--";
      paperWinRateEl.textContent = "--";
      paperNetEl.textContent = "NA";
    }

    const strategyActive = paper && typeof paper.active_trade === "object" ? paper.active_trade : null;
    if (strategyActive) {
      const side = String(strategyActive.direction || "").toUpperCase();
      const entry = formatIst(strategyActive.entry_ts);
      const source = String(strategyActive.pnl_source || "option_quote");
      const markLabel = source.indexOf("option_") === 0 ? "Option LTP" : "Mark";
      const mark = safeNum(strategyActive.mark_price, 0).toFixed(2);
      const upnl = (safeNum(strategyActive.unrealized_points, 0) * qtyMultiplier).toFixed(2);
      const contract = homeOptionLabelFromSignal(strategyActive.signal);
      if (source === "option_quote_pending") {
        paperActiveTradeEl.textContent = `Active: ${side} | ${contract} | Entry ${entry} | Awaiting option quote...`;
      } else {
        paperActiveTradeEl.textContent = `Active: ${side} | ${contract} | Entry ${entry} | ${markLabel} ${mark} | uPnL ${upnl}`;
      }
    } else if (activeRow) {
      const side = String(activeRow.direction || "--").toUpperCase();
      const symbol = String(activeRow.symbol || "--");
      const entry = String(activeRow.entry_time_ist || "--");
      paperActiveTradeEl.textContent = `Active: ${side} | ${symbol} | Entry ${entry}`;
    } else {
      paperActiveTradeEl.textContent = "No active live trade.";
    }

    if (!closedRows.length) {
      paperTradesBodyEl.innerHTML = '<tr><td colspan="7" class="small">No live closed trades for today.</td></tr>';
      return;
    }

    paperTradesBodyEl.innerHTML = closedRows.map(function (row) {
      const id = String(row.trade_id || "--");
      const direction = String(row.direction || "--").toUpperCase();
      const contract = String(row.symbol || "--");
      const entry = String(row.entry_time_ist || "--");
      const exit = String(row.exit_time_ist || "--");
      const key = toTradeIdKey(row.trade_id);
      const linked = strategyByTradeId[key];
      const linkedPoints = safeNum(linked && linked.points, NaN) * qtyMultiplier;
      const points = Number.isFinite(linkedPoints) ? linkedPoints.toFixed(2) : "--";
      const outcome = String((linked && linked.outcome) || row.outcome || row.status || "closed");
      return `<tr>
        <td>${id}</td>
        <td>${direction}</td>
        <td>${contract}</td>
        <td>${entry}</td>
        <td>${exit}</td>
        <td>${points}</td>
        <td>${outcome}</td>
      </tr>`;
    }).join("");
  }

  async function refreshHomeLive() {
    const today = formatTodayIstDate();
    const qs = `start_date=${encodeURIComponent(today)}&end_date=${encodeURIComponent(today)}&mode=live`;
    const results = await Promise.all([
      requestJson(`/user/report/ai-trades?${qs}`),
      requestJson("/paper/state"),
    ]);
    renderLiveHomeState(results[0], results[1]);
  }

  function renderPaperDiagnostics(diagnosticsRaw) {
    if (!paperGateListEl || !paperGateLastEl) {
      return;
    }
    const diagnostics = diagnosticsRaw && typeof diagnosticsRaw === "object" ? diagnosticsRaw : {};
    const counts = diagnostics.counts && typeof diagnostics.counts === "object" ? diagnostics.counts : {};
    const lastEvent = diagnostics.last_event && typeof diagnostics.last_event === "object"
      ? diagnostics.last_event
      : {};
    const lastReason = String(lastEvent.reason || "not_evaluated");
    const lastTs = safeNum(lastEvent.ts, 0);
    const lastDetail = String(lastEvent.detail || "").trim();
    const tsText = lastTs > 0 ? ` @ ${formatIst(lastTs)}` : "";
    const detailText = lastDetail ? ` | ${lastDetail}` : "";
    paperGateLastEl.textContent = `Last: ${toLabel(lastReason)}${tsText}${detailText}`;

    const orderedKeys = [
      "candles_seen",
      "with_active_trade",
      "entry_opened",
      "guidance_mismatch",
      "direction_missing",
      "feedback_block",
      "confidence_below_min",
      "noon_chop_block",
      "pain_release_detected",
      "pain_release_missing",
      "pending_release_wait",
    ];
    const rows = orderedKeys.map((name) => ({
      name: name,
      count: safeNum(counts[name], 0),
    }));
    const hasAny = rows.some((row) => row.count > 0);
    if (!hasAny) {
      paperGateListEl.innerHTML = '<p class="small">No diagnostics yet.</p>';
      return;
    }
    paperGateListEl.innerHTML = rows.map((row) => `
      <div class="feature">
        <div class="feature-name">
          <span>${toLabel(row.name)}</span>
          <span class="mono">${row.count}</span>
        </div>
      </div>
    `).join("");
  }

  function setBacktestStatus(message) {
    if (backtestStatusEl) {
      backtestStatusEl.textContent = String(message || "");
    }
  }

  function setBacktestProgress(pct, label) {
    const value = Math.max(0, Math.min(100, safeNum(pct, 0)));
    if (backtestProgressFillEl) {
      backtestProgressFillEl.style.width = `${value}%`;
    }
    if (backtestProgressTextEl) {
      const text = String(label || "").trim();
      backtestProgressTextEl.textContent = text
        ? `Progress: ${value.toFixed(1)}% | ${text}`
        : `Progress: ${value.toFixed(1)}%`;
    }
  }

  function setBacktestDefaults() {
    if (!backtestStartEl || !backtestEndEl) {
      return;
    }
    if (backtestStartEl.value && backtestEndEl.value) {
      return;
    }
    const nowTs = Math.floor(Date.now() / 1000);
    const startTs = nowTs - (30 * 24 * 60 * 60);
    backtestStartEl.value = toDateInput(startTs);
    backtestEndEl.value = toDateInput(nowTs);
    setBacktestProgress(0, "idle");
  }

  function optionLabelFromSignal(signalObj) {
    const signal = signalObj && typeof signalObj === "object" ? signalObj : {};
    const selected = signal.selected_option && typeof signal.selected_option === "object"
      ? signal.selected_option
      : null;
    if (!selected) {
      return "--";
    }
    // Prefer the actual option symbol if available
    const sym = String(selected.symbol || selected.tradingsymbol || "").trim();
    if (sym) {
      return sym;
    }
    const side = String(selected.side || "").toUpperCase();
    const strike = safeNum(selected.strike, NaN);
    const optType = String(selected.option_type || selected.type || "").toUpperCase();
    if (!Number.isFinite(strike)) {
      return side || "--";
    }
    const strikeText = strike % 1 === 0 ? strike.toFixed(0) : strike.toFixed(2);
    return optType ? `${strikeText}${optType}` : `${side} ${strikeText}`;
  }

  function renderBacktest(result) {
    const summary = result && typeof result.summary === "object" ? result.summary : {};
    const trades = Array.isArray(result && result.trades) ? result.trades : [];

    if (backtestCountEl) {
      backtestCountEl.textContent = String(safeNum(summary.closed_trades, 0));
    }
    if (backtestWinRateEl) {
      backtestWinRateEl.textContent = `${safeNum(summary.win_rate_pct, 0).toFixed(2)}%`;
    }
    if (backtestNetEl) {
      backtestNetEl.textContent = safeNum(summary.net_points, 0).toFixed(2);
    }
    if (backtestProfitFactorEl) {
      const pf = summary.profit_factor;
      backtestProfitFactorEl.textContent = Number.isFinite(Number(pf)) ? safeNum(pf, 0).toFixed(2) : "--";
    }
    if (backtestMaxDdEl) {
      backtestMaxDdEl.textContent = safeNum(summary.max_drawdown, 0).toFixed(2);
    }
    if (backtestGrossProfitEl) {
      backtestGrossProfitEl.textContent = safeNum(summary.gross_profit, 0).toFixed(2);
    }
    if (backtestGrossLossEl) {
      backtestGrossLossEl.textContent = safeNum(summary.gross_loss, 0).toFixed(2);
    }
    if (backtestWinEl) {
      backtestWinEl.textContent = String(safeNum(summary.profit_trades, 0));
    }
    if (backtestLossEl) {
      backtestLossEl.textContent = String(safeNum(summary.loss_trades, 0));
    }
    if (backtestFlatEl) {
      backtestFlatEl.textContent = String(safeNum(summary.flat_trades, 0));
    }
    if (backtestLongEl) {
      backtestLongEl.textContent = String(safeNum(summary.long_trades, 0));
    }
    if (backtestShortEl) {
      backtestShortEl.textContent = String(safeNum(summary.short_trades, 0));
    }

    if (backtestReasonListEl) {
      const reasons = summary.close_reason_counts && typeof summary.close_reason_counts === "object"
        ? Object.entries(summary.close_reason_counts)
        : [];
      if (!reasons.length) {
        backtestReasonListEl.innerHTML = '<p class="small">No close reasons yet.</p>';
      } else {
        backtestReasonListEl.innerHTML = reasons
          .sort((a, b) => safeNum(b[1], 0) - safeNum(a[1], 0))
          .map(([name, count]) => `
            <div class="feature">
              <div class="feature-name">
                <span>${toLabel(name)}</span>
                <span class="mono">${safeNum(count, 0)}</span>
              </div>
            </div>
          `)
          .join("");
      }
    }

    if (backtestTradesBodyEl) {
      if (!trades.length) {
        backtestTradesBodyEl.innerHTML = '<tr><td colspan="7" class="small">No trades in selected range.</td></tr>';
      } else {
        const recent = trades.slice().reverse();
        backtestTradesBodyEl.innerHTML = recent.map((row) => `
          <tr>
            <td>${safeNum(row.trade_id, 0)}</td>
            <td>${String(row.direction || "--")}</td>
            <td>${formatIst(row.entry_ts)}</td>
            <td>${formatIst(row.exit_ts)}</td>
            <td>${safeNum(row.points, 0).toFixed(2)}</td>
            <td>${String(row.outcome || "flat")}</td>
            <td>${toLabel(row.close_reason || "unknown")} | ${optionLabelFromSignal(row.signal)}</td>
          </tr>
        `).join("");
      }
    }

    const windowInfo = result && typeof result.window === "object" ? result.window : {};
    const modelInfo = result && typeof result.model === "object" ? result.model : {};
    const dataInfo = result && typeof result.data === "object" ? result.data : {};
    const paperConfig = result && typeof result.paper_config === "object" ? result.paper_config : {};
    setBacktestProgress(100, "completed");
    setBacktestStatus(
      `Done. Window ${windowInfo.data_start_ist || "--"} -> ${windowInfo.data_end_ist || "--"} | `
      + `1m ${safeNum(dataInfo.candles_1m, 0)} | 5m ${safeNum(dataInfo.candles_5m, 0)} | `
      + `opt ${safeNum(dataInfo.option_snapshots, 0)} | model ${modelInfo.analysis_timeframe || "5m"} primary | `
      + `pain_release ${paperConfig.require_pain_release ? "ON" : "OFF"}`,
    );
  }

  async function onBacktestRun() {
    if (!backtestRunBtn) {
      return;
    }
    setBacktestDefaults();
    const startValue = String((backtestStartEl && backtestStartEl.value) || "").trim();
    const endValue = String((backtestEndEl && backtestEndEl.value) || "").trim();
    if (!startValue || !endValue) {
      setBacktestStatus("Backtest start/end is required.");
      return;
    }
    if (endValue < startValue) {
      setBacktestStatus("Backtest end date must be on or after start date.");
      return;
    }
    const maxCandles = Math.max(500, safeNum(backtestMaxCandlesEl && backtestMaxCandlesEl.value, 120000));
    const maxTrades = Math.max(100, safeNum(backtestMaxTradesEl && backtestMaxTradesEl.value, 1000));
    const closeOpen = Boolean(backtestCloseOpenEl && backtestCloseOpenEl.checked);
    const requireRelease = false;
    const startPayload = `${startValue}T00:00:00`;
    const endPayload = `${endValue}T23:59:59`;

    backtestRunBtn.disabled = true;
    if (backtestLoadBtn) {
      backtestLoadBtn.disabled = true;
    }
    setBacktestProgress(0, "starting");
    setBacktestStatus("Running backtest...");
    try {
      const started = await requestJson("/backtest/run", {
        method: "POST",
        data: {
          start: startPayload,
          end: endPayload,
          max_candles: maxCandles,
          max_trades: maxTrades,
          close_open_at_end: closeOpen,
          require_pain_release: requireRelease,
        },
      });
      const job = started && typeof started.job === "object" ? started.job : {};
      const jobId = String(job.job_id || "").trim();
      if (!jobId) {
        throw new Error("Backtest job id missing");
      }
      activeBacktestJobId = jobId;
      await pollBacktestProgress(jobId);
    } catch (err) {
      activeBacktestJobId = "";
      setBacktestStatus(`Backtest failed: ${err.message}`);
      setBacktestProgress(0, "failed");
    } finally {
      backtestRunBtn.disabled = false;
      if (backtestLoadBtn) {
        backtestLoadBtn.disabled = false;
      }
    }
  }

  async function onBacktestLoadLatest() {
    if (!backtestLoadBtn) {
      return;
    }
    backtestLoadBtn.disabled = true;
    setBacktestStatus("Loading latest backtest...");
    try {
      if (activeBacktestJobId) {
        try {
          const info = await requestJson(`/backtest/progress?job_id=${encodeURIComponent(activeBacktestJobId)}`);
          if (String(info.status || "").toLowerCase() === "running") {
            await pollBacktestProgress(activeBacktestJobId);
            return;
          }
        } catch (_err) {
          // Ignore and fall back to latest result fetch.
        }
      }
      const result = await requestJson("/backtest/latest");
      renderBacktest(result);
    } catch (err) {
      setBacktestStatus(`Load failed: ${err.message}`);
    } finally {
      backtestLoadBtn.disabled = false;
    }
  }

  async function pollBacktestProgress(jobId) {
    const id = String(jobId || "").trim();
    if (!id) {
      throw new Error("Invalid backtest job id");
    }
    let attempts = 0;
    while (attempts < 1200) {
      attempts += 1;
      const info = await requestJson(`/backtest/progress?job_id=${encodeURIComponent(id)}`);
      const progress = safeNum(info.progress_pct, 0);
      const statusText = String(info.message || info.status || "running");
      setBacktestProgress(progress, statusText);
      if (String(info.status || "").toLowerCase() === "completed") {
        const result = await requestJson("/backtest/latest");
        renderBacktest(result);
        activeBacktestJobId = "";
        return;
      }
      if (String(info.status || "").toLowerCase() === "failed") {
        activeBacktestJobId = "";
        throw new Error(String(info.error || "Backtest failed"));
      }
      await new Promise((resolve) => setTimeout(resolve, 900));
    }
    throw new Error("Backtest progress timeout");
  }

  async function refreshPaper() {
    try {
      if (isHomeLiveMode()) {
        await refreshHomeLive();
      } else {
        const paper = await requestJson("/paper/state");
        renderPaperState(paper);
      }
    } catch (_err) {
      if (paperBackendEl) {
        paperBackendEl.textContent = "Backend: unavailable";
      }
      if (paperActiveTradeEl && isHomeLiveMode()) {
        paperActiveTradeEl.textContent = "Live trades unavailable.";
      }
      if (paperTradesBodyEl && isHomeLiveMode()) {
        paperTradesBodyEl.innerHTML = '<tr><td colspan="7" class="small">Live trade data unavailable.</td></tr>';
      }
    }
  }

  function retrainLearningText(row) {
    const item = row && typeof row === "object" ? row : {};
    const learned = String(item.learned_text || "").trim();
    if (learned) {
      return learned;
    }
    const failed = String(item.failure_text || "").trim();
    if (failed) {
      return failed;
    }
    const reason = String(item.reason || "").trim();
    if (reason) {
      return reason;
    }
    return "--";
  }

  function renderRetrainStatus(payload) {
    if (!retrainStatusViewEl) {
      return;
    }
    const status = payload && typeof payload === "object" ? payload : {};
    const last = status.last_result && typeof status.last_result === "object" ? status.last_result : {};
    const lines = [
      `enabled=${Boolean(status.enabled)} running=${Boolean(status.running)} store_ready=${Boolean(status.run_store_ready)}`,
      `daily_time=${String(status.daily_time || "--")} interval_sec=${safeNum(status.interval_sec, 0)}`,
      `run_count=${safeNum(status.run_count, 0)} success=${safeNum(status.success_count, 0)} failure=${safeNum(status.failure_count, 0)}`,
      `last_started=${formatIst(status.last_started_ts)} last_finished=${formatIst(status.last_finished_ts)} last_success=${formatIst(status.last_success_ts)}`,
      `last_status=${String(last.status || "--")} deployed=${Boolean(last.deployed)} reason=${String(last.reason || last.message || last.error || "--")}`,
    ];
    retrainStatusViewEl.textContent = lines.join("\n");
  }

  function renderRetrainHistory(rows) {
    if (!retrainHistoryBodyEl) {
      return;
    }
    const list = Array.isArray(rows) ? rows : [];
    if (!list.length) {
      retrainHistoryBodyEl.innerHTML = '<tr><td colspan="6" class="small">No retrain history yet.</td></tr>';
      return;
    }
    retrainHistoryBodyEl.innerHTML = list.map(function (row) {
      const started = formatIst(row.started_at);
      const trigger = String(row.trigger || "--");
      const status = String(row.status || "--");
      const deployed = Boolean(row.deployed) ? "Yes" : "No";
      const rowsText = `${safeNum(row.candles_rows, 0)} / ${safeNum(row.options_rows, 0)}`;
      const learning = retrainLearningText(row);
      return `
        <tr>
          <td>${started}</td>
          <td>${trigger}</td>
          <td>${status}</td>
          <td>${deployed}</td>
          <td>${rowsText}</td>
          <td>${learning}</td>
        </tr>
      `;
    }).join("");
  }

  function formatMistakeBucket(name) {
    const key = String(name || "none").trim().toLowerCase();
    if (!key) {
      return "None";
    }
    if (key === "none") {
      return "None";
    }
    return key.split("_").map(function (part) {
      if (!part) return "";
      return part.charAt(0).toUpperCase() + part.slice(1);
    }).join(" ");
  }

  function renderRetrainMistakes(payload) {
    if (retrainMistakeSummaryEl) {
      const summary = payload && typeof payload.summary === "object" ? payload.summary : {};
      const totals = summary.totals && typeof summary.totals === "object" ? summary.totals : {};
      const mistakeTotals = summary.mistake_totals && typeof summary.mistake_totals === "object" ? summary.mistake_totals : {};
      const days = Math.max(1, safeNum(summary.days, 30));
      const generated = formatIst(summary.generated_at);
      retrainMistakeSummaryEl.textContent =
        `Window=${days}D | Generated=${generated}\n`
        + `All trades: ${safeNum(totals.trades, 0)} | Win% ${safeNum(totals.win_rate_pct, 0).toFixed(2)} | Net ${safeNum(totals.net_points, 0).toFixed(2)}\n`
        + `Mistake-only: ${safeNum(mistakeTotals.trades, 0)} | Win% ${safeNum(mistakeTotals.win_rate_pct, 0).toFixed(2)} | Net ${safeNum(mistakeTotals.net_points, 0).toFixed(2)}`;
    }
    if (!retrainMistakeBodyEl) {
      return;
    }
    const summary = payload && typeof payload.summary === "object" ? payload.summary : {};
    const buckets = Array.isArray(summary.buckets) ? summary.buckets : [];
    if (!buckets.length) {
      retrainMistakeBodyEl.innerHTML = '<tr><td colspan="7" class="small">No mistake analytics yet.</td></tr>';
      return;
    }
    retrainMistakeBodyEl.innerHTML = buckets.map(function (row) {
      const net = safeNum(row.net_points, 0);
      const avg = safeNum(row.avg_points, 0);
      const netText = `${net >= 0 ? "+" : ""}${net.toFixed(2)}`;
      const avgText = `${avg >= 0 ? "+" : ""}${avg.toFixed(2)}`;
      const topReason = String(row.top_close_reason || "--");
      return `
        <tr>
          <td>${formatMistakeBucket(row.mistake_type)}</td>
          <td>${safeNum(row.trades, 0)}</td>
          <td>${safeNum(row.win_rate_pct, 0).toFixed(2)}%</td>
          <td>${netText}</td>
          <td>${avgText}</td>
          <td>${safeNum(row.avg_mistake_score, 0).toFixed(3)}</td>
          <td>${topReason}</td>
        </tr>
      `;
    }).join("");
  }

  async function refreshRetrainPanel() {
    if (!hasRetrainUi() || retrainBusy) {
      return;
    }
    retrainBusy = true;
    if (retrainRefreshBtn) {
      retrainRefreshBtn.disabled = true;
    }
    try {
      const [statusPayload, historyPayload, mistakesPayload] = await Promise.all([
        requestJson("/ml/retrain/status"),
        requestJson("/ml/retrain/history?limit=60"),
        requestJson("/ml/retrain/mistakes/summary?days=30&limit=5000"),
      ]);
      renderRetrainStatus(statusPayload);
      const rows = historyPayload && Array.isArray(historyPayload.history) ? historyPayload.history : [];
      renderRetrainHistory(rows);
      renderRetrainMistakes(mistakesPayload);
    } catch (err) {
      if (retrainStatusViewEl) {
        retrainStatusViewEl.textContent = `Retrain status failed: ${err.message}`;
      }
      renderRetrainHistory([]);
      renderRetrainMistakes({});
    } finally {
      if (retrainRefreshBtn) {
        retrainRefreshBtn.disabled = false;
      }
      retrainBusy = false;
    }
  }

  function renderFyersStatus(payload) {
    if (!fyersStatusViewEl) {
      return;
    }
    fyersStatusViewEl.textContent = JSON.stringify(payload || {}, null, 2);
  }

  async function refreshFyersStatus() {
    if (!hasFyersUi) {
      return;
    }
    try {
      const statusObj = await requestJson("/auth/fyers/status");
      renderFyersStatus(statusObj);
      setText(fyersMsgEl, "FYERS status loaded.");
    } catch (err) {
      setText(fyersMsgEl, `Status failed: ${err.message}`);
    }
  }

  async function onFyersUrl() {
    if (!hasFyersUi) {
      return;
    }
    try {
      const data = await requestJson("/auth/fyers/url");
      const url = String(data.auth_url || "").trim();
      if (!url) {
        setText(fyersMsgEl, "No login URL received.");
        return;
      }
      window.open(url, "_blank", "noopener");
      setText(fyersMsgEl, "FYERS login URL opened.");
    } catch (err) {
      setText(fyersMsgEl, `URL failed: ${err.message}`);
    }
  }

  async function onFyersExchange() {
    if (!hasFyersUi) {
      return;
    }
    const authCode = String((fyersAuthCodeEl && fyersAuthCodeEl.value) || "").trim();
    if (!authCode) {
      setText(fyersMsgEl, "Auth code is required.");
      return;
    }
    try {
      await requestJson("/auth/fyers/exchange", {
        method: "POST",
        data: { auth_code: authCode },
      });
      if (fyersAuthCodeEl) {
        fyersAuthCodeEl.value = "";
      }
      setText(fyersMsgEl, "Auth code exchanged successfully.");
      await refreshFyersStatus();
    } catch (err) {
      setText(fyersMsgEl, `Exchange failed: ${err.message}`);
    }
  }

  async function onFyersRefresh() {
    if (!hasFyersUi) {
      return;
    }
    try {
      const data = await requestJson("/auth/fyers/refresh", {
        method: "POST",
        data: { force: true },
      });
      if (data && data.refreshed) {
        setText(fyersMsgEl, "Token refreshed.");
      } else {
        setText(fyersMsgEl, "Refresh skipped (not needed yet).");
      }
      await refreshFyersStatus();
    } catch (err) {
      setText(fyersMsgEl, `Refresh failed: ${err.message}`);
    }
  }

  async function refreshMarketChart(forceFull) {
    if (!hasChartUi) {
      return;
    }
    if (marketBusy) {
      return;
    }
    marketBusy = true;
    try {
      const since = (!forceFull && chartHistoryLoaded && chartLastBarTime > 0)
        ? Math.max(0, chartLastBarTime - (chartTf === "5m" ? 900 : 300))
        : 0;
      const path = `/market/candles?timeframe=${encodeURIComponent(chartTf)}&limit=800&since=${since}`;
      const market = await requestJson(path);
      renderMarketChart(market);
    } catch (marketErr) {
      setText(chartStatusEl, `Chart unavailable: ${marketErr.message}`);
    } finally {
      marketBusy = false;
    }
  }

  async function refreshAll() {
    if (coreBusy) {
      return;
    }
    coreBusy = true;
    try {
      const [health, state, raw] = await Promise.all([
        requestJson("/health"),
        requestJson("/state"),
        requestJson("/state/raw"),
      ]);
      setText(apiStatusEl, `API: ${health.status || "ok"}`);
      setText(streamStatusEl, raw && raw.timestamp ? "State: live" : "State: warming up");
      applySnapshot(raw, state);
      await refreshPaper();
    } catch (err) {
      setText(apiStatusEl, "API: down");
      setText(streamStatusEl, "State: unavailable");
      console.error(err);
    } finally {
      coreBusy = false;
    }
  }

  async function refreshReady() {
    if (readyBusy) {
      return;
    }
    readyBusy = true;
    try {
      const payload = await requestJson("/ready");
      const ready = Boolean(payload && payload.ready);
      const issues = Array.isArray(payload && payload.issues) ? payload.issues : [];
      const fyers = (payload && payload.fyers) || {};
      const accessLeft = formatReadyDuration(fyers.seconds_to_access_expiry || 0);
      const refreshLeft = formatReadyDuration(fyers.seconds_to_refresh_expiry || 0);
      if (ready) {
        setText(readyStatusEl, "Ready: PASS");
        setPillState(readyStatusEl, "good");
        setText(readySummaryEl, `All checks passed. Access ${accessLeft}, refresh ${refreshLeft}.`);
      } else if (issues.length) {
        setText(readyStatusEl, "Ready: ATTN");
        setPillState(readyStatusEl, "warn");
        setText(readySummaryEl, issues[0]);
      } else {
        setText(readyStatusEl, "Ready: FAIL");
        setPillState(readyStatusEl, "bad");
        setText(readySummaryEl, "Readiness checks failed.");
      }
    } catch (err) {
      setText(readyStatusEl, "Ready: unavailable");
      setPillState(readyStatusEl, "bad");
      setText(readySummaryEl, `Readiness check failed: ${err.message}`);
    } finally {
      readyBusy = false;
    }
  }

  async function requireSession() {
    try {
      const me = await requestJson("/auth/me");
      currentUser = (me && me.user && typeof me.user === "object") ? me.user : null;
      setUserLine(currentUser);
      return true;
    } catch (_err) {
      currentUser = null;
      window.location.replace("/");
      return false;
    }
  }

  async function onLogout() {
    currentUser = null;
    try {
      await requestJson("/auth/logout", { method: "POST" });
    } catch (_err) {
      // Redirect anyway to clear UI state.
    }
    window.location.replace("/");
  }

  async function onPaperReset() {
    if (!paperResetBtn) {
      return;
    }
    paperResetBtn.disabled = true;
    try {
      await requestJson("/paper/reset", {
        method: "POST",
        data: { close_open: false },
      });
      await refreshPaper();
    } catch (err) {
      console.error(err);
    } finally {
      paperResetBtn.disabled = false;
    }
  }

  clearLogBtn.addEventListener("click", function () {
    timeline = [];
    timelineEl.innerHTML = "";
  });
  if (logoutBtn) {
    logoutBtn.addEventListener("click", onLogout);
  }
  if (paperResetBtn) {
    paperResetBtn.addEventListener("click", onPaperReset);
  }
  if (hasFyersUi && fyersStatusBtn) {
    fyersStatusBtn.addEventListener("click", refreshFyersStatus);
  }
  if (hasFyersUi && fyersUrlBtn) {
    fyersUrlBtn.addEventListener("click", onFyersUrl);
  }
  if (hasFyersUi && fyersExchangeBtn) {
    fyersExchangeBtn.addEventListener("click", onFyersExchange);
  }
  if (hasFyersUi && fyersRefreshBtn) {
    fyersRefreshBtn.addEventListener("click", onFyersRefresh);
  }
  if (hasChartUi && tf1mBtn) {
    tf1mBtn.addEventListener("click", function () { onTfChange("1m"); });
  }
  if (hasChartUi && tf5mBtn) {
    tf5mBtn.addEventListener("click", function () { onTfChange("5m"); });
  }
  if (backtestRunBtn) {
    backtestRunBtn.addEventListener("click", onBacktestRun);
  }
  if (backtestLoadBtn) {
    backtestLoadBtn.addEventListener("click", onBacktestLoadLatest);
  }
  if (retrainRefreshBtn) {
    retrainRefreshBtn.addEventListener("click", refreshRetrainPanel);
  }
  if (sidebarToggleBtn) {
    sidebarToggleBtn.addEventListener("click", function () {
      const isOpen = document.body.classList.contains("sidebar-open");
      setSidebarOpen(!isOpen);
    });
  }
  if (sidebarCloseBtn) {
    sidebarCloseBtn.addEventListener("click", function () {
      setSidebarOpen(false);
    });
  }
  if (sidebarOverlayEl) {
    sidebarOverlayEl.addEventListener("click", function () {
      setSidebarOpen(false);
    });
  }
  viewNavButtons.forEach(function (btn) {
    btn.addEventListener("click", function () {
      const viewTarget = String(btn.dataset.viewTarget || "home");
      setActiveView(viewTarget);
      if (viewTarget === "broker") {
        refreshBrokerClientId();
      } else if (viewTarget === "lotconfig") {
        refreshAdminLotSize();
      } else if (viewTarget === "angelhits") {
        setAngelApiDefaults();
        loadAngelApiHits();
      } else if (viewTarget === "profile") {
        refreshUserLotConfig();
      } else if (viewTarget === "report") {
        setReportDefaults();
        loadReportTrades();
      }
    });
  });
  if (profileToggleBtn) {
    profileToggleBtn.addEventListener("click", function (event) {
      event.stopPropagation();
      const isOpen = profileMenuEl && !profileMenuEl.hidden;
      setProfileMenuOpen(!isOpen);
    });
  }
  if (profileLogoutBtn) {
    profileLogoutBtn.addEventListener("click", function () {
      setProfileMenuOpen(false);
      onLogout();
    });
  }
  if (brokerClientIdSaveBtn) {
    brokerClientIdSaveBtn.addEventListener("click", onBrokerClientIdSave);
  }
  if (brokerClientIdInputEl) {
    brokerClientIdInputEl.addEventListener("keydown", function (event) {
      if (event.key === "Enter") {
        event.preventDefault();
        onBrokerClientIdSave();
      }
    });
  }
  [brokerApiKeyInputEl, brokerPinInputEl, brokerTotpInputEl].forEach(function (inputEl) {
    if (!inputEl) {
      return;
    }
    inputEl.addEventListener("keydown", function (event) {
      if (event.key === "Enter") {
        event.preventDefault();
        onBrokerClientIdSave();
      }
    });
  });
  if (terminalLoginToggleEl) {
    terminalLoginToggleEl.addEventListener("click", onTerminalLoginToggle);
  }
  if (subscribeNowBtn) {
    subscribeNowBtn.addEventListener("click", onSubscribeNow);
  }
  if (subscribeModalOpenBtnEl) {
    subscribeModalOpenBtnEl.addEventListener("click", function () {
      updateSubscribePlanUi();
      setSubscribeModalOpen(true);
    });
  }
  if (subscribeModalCloseBtnEl) {
    subscribeModalCloseBtnEl.addEventListener("click", function () {
      setSubscribeModalOpen(false);
    });
  }
  subscribeModalCloseEls.forEach(function (el) {
    el.addEventListener("click", function () {
      setSubscribeModalOpen(false);
    });
  });
  subscribePlanButtons.forEach(function (btn) {
    btn.addEventListener("click", function () {
      const code = normalizePlanCode(btn.dataset.subscribePlan || "");
      updateSubscribePlanUi(code);
      onSubscribeNow(code);
    });
  });
  if (subscribePlanSelectEl) {
    subscribePlanSelectEl.addEventListener("change", updateSubscribePlanUi);
  }
  if (tradingEngineToggleEl) {
    tradingEngineToggleEl.addEventListener("click", onTradingEngineToggle);
  }
  if (userLotCountSaveBtn) {
    userLotCountSaveBtn.addEventListener("click", onUserLotCountSave);
  }
  if (userLotCountInputEl) {
    userLotCountInputEl.addEventListener("keydown", function (event) {
      if (event.key === "Enter") {
        event.preventDefault();
        onUserLotCountSave();
      }
    });
  }
  if (adminLotSizeSaveBtn) {
    adminLotSizeSaveBtn.addEventListener("click", onAdminLotSizeSave);
  }
  if (adminLotSizeInputEl) {
    adminLotSizeInputEl.addEventListener("keydown", function (event) {
      if (event.key === "Enter") {
        event.preventDefault();
        onAdminLotSizeSave();
      }
    });
  }
  if (angelApiLoadBtn) {
    angelApiLoadBtn.addEventListener("click", loadAngelApiHits);
  }
  if (reportLoadBtn) {
    reportLoadBtn.addEventListener("click", loadReportTrades);
  }
  if (reportEndDateEl) {
    reportEndDateEl.addEventListener("change", function () {
      if (!reportStartDateEl) {
        return;
      }
      reportStartDateEl.max = reportEndDateEl.value || "";
      if (reportStartDateEl.value && reportEndDateEl.value && reportStartDateEl.value > reportEndDateEl.value) {
        reportStartDateEl.value = reportEndDateEl.value;
      }
    });
  }
  document.addEventListener("click", function (event) {
    if (!profileWrapEl || !profileMenuEl || profileMenuEl.hidden) {
      return;
    }
    const target = event.target;
    if (target instanceof Node && !profileWrapEl.contains(target)) {
      setProfileMenuOpen(false);
    }
  });
  sidebarNavLinks.forEach(function (link) {
    link.addEventListener("click", function () {
      setSidebarOpen(false);
    });
  });
  window.addEventListener("resize", function () {
    if (!isMobileSidebarMode()) {
      setSidebarOpen(false);
    }
  });
  window.addEventListener("scroll", syncTopbarScrollState, { passive: true });
  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape") {
      setProfileMenuOpen(false);
      setSidebarOpen(false);
      setSubscribeModalOpen(false);
    }
  });

  async function init() {
    const loggedIn = await requireSession();
    if (!loggedIn) {
      return;
    }
    setBacktestDefaults();
    setReportDefaults();
    setAngelApiDefaults();
    setBacktestProgress(0, "idle");
    setSidebarOpen(false);
    setActiveView("home");
    setProfileMenuOpen(false);
    setSubscribeModalOpen(false);
    syncTopbarScrollState();
    setTerminalLoginConnected(false);
    setTradingEngineEnabled(false);
    updateSubscribePlanUi();
    await refreshBrokerClientId();
    if (hasUserLotUi()) {
      await refreshUserLotConfig();
    }
    if (hasAdminLotUi()) {
      await refreshAdminLotSize();
    }
    applyAngelCallbackMessage();
    if (hasChartUi) {
      setTfButtons();
      initChart();
    }
    await refreshAll();
    await refreshReady();
    if (hasRetrainUi()) {
      await refreshRetrainPanel();
    }
    if (hasChartUi) {
      await refreshMarketChart(true);
    }
    if (hasFyersUi) {
      await refreshFyersStatus();
    }
    setInterval(refreshAll, 2500);
    setInterval(refreshReady, 15000);
    if (hasRetrainUi()) {
      setInterval(refreshRetrainPanel, 30000);
    }
    if (hasChartUi) {
      setInterval(function () { refreshMarketChart(false); }, 800);
    }
  }

  init();
})();
