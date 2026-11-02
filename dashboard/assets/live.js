/* Live scenarios (serverless mode): submit synthetic orders, tick the processor,
   show where each one ended and why. Only loaded when live mode is on. */
(function () {
  "use strict";
  const NEEDS_TOKEN = document.getElementById("livebox").dataset.needsToken === "true";
  const POLL_MS = Math.max(2000, Number(document.body.dataset.pollMs) || 2000);
  let lastTick = 0;
  let busy = false;
  let catalog = {};
  const $ = (id) => document.getElementById(id);

  function cell(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }
  function say(text, isErr) {
    const m = $("liveMsg");
    m.textContent = text;
    m.className = "livemsg" + (isErr ? " err" : "");
  }
  function storedToken() { try { return localStorage.getItem("verisyncWriteToken") || ""; } catch (e) { return ""; } }
  function storeToken(v) { try { localStorage.setItem("verisyncWriteToken", v); } catch (e) { /* private mode */ } }

  function showWhy() {
    const s = catalog[$("liveScenario").value];
    $("liveWhy").textContent = s ? "Expected: " + s.why : "";
  }

  async function loadCatalog() {
    const res = await fetch("/api/live/scenarios", { cache: "no-store" });
    if (!res.ok) return;
    const body = await res.json();
    const sel = $("liveScenario");
    for (const s of body.scenarios) {
      catalog[s.key] = s;
      const o = document.createElement("option");
      o.value = s.key;
      o.textContent = s.label;
      sel.appendChild(o);
    }
    $("liveCount").max = body.max_per_request;
    showWhy();
  }

  function renderOrders(orders) {
    const body = $("liveRows");
    body.replaceChildren();
    if (!orders.length) {
      const tr = cell("tr");
      const td = cell("td", "sub", "No live orders yet. Submit one above.");
      td.colSpan = 4;
      tr.appendChild(td);
      body.appendChild(tr);
      return;
    }
    for (const o of orders) {
      const tr = cell("tr");
      tr.appendChild(cell("td", "oid mono", o.order_id.replace("order_LIVE_", "")));
      tr.appendChild(cell("td", "", o.scenario_label || "?"));
      const oc = cell("td");
      oc.appendChild(cell("span", "otag " + o.outcome.tone, o.outcome.label));
      if (o.recheck_in_seconds != null) oc.appendChild(cell("span", "match", "recheck in " + o.recheck_in_seconds + "s"));
      if (o.outcome.terminal && o.expected_tag) {
        const ok = o.outcome.tag === o.expected_tag;
        oc.appendChild(cell("span", "match " + (ok ? "yes" : "no"), ok ? "as expected" : "unexpected"));
      }
      tr.appendChild(oc);
      let why = o.why || "";
      if (!o.outcome.terminal) why = "Still in progress. " + why;
      tr.appendChild(cell("td", "why", why));
      body.appendChild(tr);
    }
  }

  async function refreshOrders() {
    const res = await fetch("/api/live/orders", { cache: "no-store" });
    if (res.ok) renderOrders((await res.json()).orders);
  }

  // Tick while the live or activity tab is open, at most once per POLL_MS.
  async function pump() {
    const tab = window.verisync.activeTab();
    if (busy || document.hidden || (tab !== "live" && tab !== "activity")) return;
    busy = true;
    try {
      if (Date.now() - lastTick >= POLL_MS - 50) {
        lastTick = Date.now();
        await fetch("/api/tick", { method: "POST" });
      }
      if (tab === "live") await refreshOrders();
    } catch (e) { /* the activity tab reports connectivity */ } finally { busy = false; }
  }

  $("liveScenario").addEventListener("change", showWhy);
  $("liveForm").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const btn = $("liveGo");
    btn.disabled = true;
    say("submitting...", false);
    const headers = { "Content-Type": "application/json" };
    if (NEEDS_TOKEN) {
      const t = $("liveToken").value;
      storeToken(t);
      headers["X-Write-Token"] = t;
    }
    try {
      const res = await fetch("/api/orders", {
        method: "POST",
        headers,
        body: JSON.stringify({
          amount: Math.round(Number($("liveAmount").value) * 100),
          scenario: $("liveScenario").value,
          count: Number($("liveCount").value),
        }),
      });
      const body = await res.json().catch(() => ({}));
      if (res.ok) {
        say("created " + body.created + " order(s)", false);
        lastTick = 0;
        await refreshOrders();
      } else {
        const d = body.detail;
        say(typeof d === "string" ? d : "request rejected (" + res.status + ")", true);
      }
    } catch (e) {
      say("request failed", true);
    } finally {
      btn.disabled = false;
    }
  });

  if (NEEDS_TOKEN) {
    $("liveTokenWrap").hidden = false;
    $("liveToken").value = storedToken();
  }
  window.verisync.onTab((name) => { if (name === "live") { refreshOrders().catch(() => {}); pump(); } });
  setInterval(pump, POLL_MS);
  loadCatalog().then(refreshOrders).catch(() => {});
})();
