/* The reconcile tool: provide data, run, review, download recommended actions.
   Everything stays in the page; the only network call is POST /api/reconcile
   (and GET /api/reconcile/sample for the sample button). */
(function () {
  "use strict";

  const MAX_BODY_BYTES = 2 * 1024 * 1024;

  const VERDICTS = [
    { key: "FLAGGED_RISKY", label: "Flagged risky", glyph: "⚠" },
    { key: "DISPUTED", label: "Disputed", glyph: "⚖" },
    { key: "CORRELATION_FAILED", label: "Correlation failed", glyph: "≠" },
    { key: "UNRESOLVED", label: "Unresolved", glyph: "?" },
    { key: "AUTO_CORRECT", label: "Auto-correct", glyph: "⇄" },
    { key: "CONSISTENT", label: "Consistent", glyph: "✓" },
  ];
  const VERDICT_ORDER = Object.fromEntries(VERDICTS.map((v, i) => [v.key, i]));
  const VERDICT_BY_KEY = Object.fromEntries(VERDICTS.map((v) => [v.key, v]));
  const REVIEW_VERDICTS = new Set(["FLAGGED_RISKY", "DISPUTED", "CORRELATION_FAILED", "UNRESOLVED"]);

  const TEMPLATES = {
    gateway:
      "order_id,status,amount,currency,payment_id,refund_id,dispute_id,event_time\r\n" +
      "order_1001,captured,1200.50,INR,pay_1001,,,\r\n",
    merchant:
      "order_id,status,amount,currency,event_time\r\n" +
      "order_1001,paid,1200.50,INR,\r\n",
  };

  const $ = (id) => document.getElementById(id);
  const state = {
    result: null,
    filter: "ALL",
    query: "",
    sort: { key: "verdict", dir: 1 },
    expanded: new Set(),
  };

  // ---------------------------------------------------------------- helpers
  function h(tag, attrs, ...kids) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (v === null || v === undefined || v === false) continue;
      if (k === "class") el.className = v;
      else if (k === "text") el.textContent = v;
      else el.setAttribute(k, v === true ? "" : v);
    }
    for (const kid of kids.flat()) {
      if (kid === null || kid === undefined || kid === false) continue;
      el.appendChild(typeof kid === "string" ? document.createTextNode(kid) : kid);
    }
    return el;
  }

  // paise (integer) -> "1,200.50" using integer arithmetic only.
  function fmtPaise(p) {
    if (p === null || p === undefined) return "—";
    const neg = p < 0;
    const abs = Math.abs(p);
    const rupees = Math.floor(abs / 100);
    const paise = String(abs % 100).padStart(2, "0");
    return (neg ? "-" : "") + rupees.toLocaleString("en-IN") + "." + paise;
  }
  function exactRupees(p) {
    if (p === null || p === undefined) return "";
    return Math.floor(p / 100) + "." + String(p % 100).padStart(2, "0");
  }
  function money(p, currency) {
    if (p === null || p === undefined) return "—";
    const sym = !currency || currency === "INR" ? "₹" : currency + " ";
    return sym + fmtPaise(p);
  }
  function bytesOf(text) { return new TextEncoder().encode(text).length; }
  function prettyBytes(n) { return n < 1024 ? n + " B" : (n / 1024).toFixed(n < 10240 ? 1 : 0) + " KB"; }
  function currentUnit() { return document.querySelector('input[name="unit"]:checked').value; }

  function stamp(key) {
    const v = VERDICT_BY_KEY[key] || { label: key, glyph: "" };
    return h("span", { class: "stamp v-" + key }, h("i", { "aria-hidden": "true", text: v.glyph }), v.label);
  }

  function setView(which) {
    $("resEmpty").hidden = which !== "empty";
    $("resLoading").hidden = which !== "loading";
    $("resError").hidden = which !== "error";
    $("resBody").hidden = which !== "body";
    $("results").setAttribute("aria-busy", which === "loading" ? "true" : "false");
  }

  // ----------------------------------------------------------- data inputs
  function fieldIds(which) { return { txt: $("txt" + which), meta: $("meta" + which), file: $("file" + which), box: $("drop" + which) }; }

  function updateMeta(which) {
    const { txt, meta } = fieldIds(which);
    const text = txt.value;
    if (!text.trim()) meta.textContent = "Empty";
    else {
      const lines = text.split(/\r\n|\r|\n/).filter((l) => l.trim()).length;
      meta.textContent = lines + " lines · " + prettyBytes(bytesOf(text));
    }
    txt.removeAttribute("aria-invalid");
    updateRunEnabled();
  }

  function updateRunEnabled() {
    const ready = $("txtGateway").value.trim() && $("txtMerchant").value.trim();
    $("run").disabled = !ready;
    if (!state.result && $("resError").hidden) {
      $("runStatus").textContent = ready ? "Ready. Press Run." : "Add both files to begin, or load the sample.";
    }
  }

  async function loadFile(which, file) {
    const { txt, meta } = fieldIds(which);
    if (!file) return;
    if (file.size > MAX_BODY_BYTES) {
      meta.textContent = "File is larger than 2 MB";
      return;
    }
    txt.value = await file.text();
    updateMeta(which);
  }

  for (const which of ["Gateway", "Merchant"]) {
    const { txt, file, box } = fieldIds(which);
    txt.addEventListener("input", () => updateMeta(which));
    file.addEventListener("change", () => { loadFile(which, file.files[0]); file.value = ""; });
    document.querySelector('[data-pick="' + which + '"]').addEventListener("click", () => file.click());
    document.querySelector('[data-clear="' + which + '"]').addEventListener("click", () => { txt.value = ""; updateMeta(which); txt.focus(); });
    for (const t of ["dragenter", "dragover"]) {
      box.addEventListener(t, (ev) => { ev.preventDefault(); box.classList.add("over"); });
    }
    for (const t of ["dragleave", "drop"]) {
      box.addEventListener(t, (ev) => { ev.preventDefault(); if (t === "dragleave" && box.contains(ev.relatedTarget)) return; box.classList.remove("over"); });
    }
    box.addEventListener("drop", (ev) => { const f = ev.dataTransfer && ev.dataTransfer.files[0]; if (f) loadFile(which, f); });
  }

  function templateLink(id, text) {
    const a = $(id);
    a.href = URL.createObjectURL(new Blob([text], { type: "text/csv" }));
  }
  templateLink("tplGateway", TEMPLATES.gateway);
  templateLink("tplMerchant", TEMPLATES.merchant);

  function updateUnitHelp() {
    $("unitHelp").textContent = currentUnit() === "rupees"
      ? "Amounts like 1200.50 mean rupees." : "Amounts like 120050 mean paise (1/100 of a rupee).";
  }
  for (const r of document.querySelectorAll('input[name="unit"]')) {
    r.addEventListener("change", () => {
      updateUnitHelp();
      if (state.result) $("runStatus").textContent = "Unit changed. Run again to update the results.";
    });
  }

  $("loadSample").addEventListener("click", async () => {
    const btn = $("loadSample");
    btn.disabled = true;
    try {
      const res = await fetch("/api/reconcile/sample", { cache: "no-store" });
      if (!res.ok) throw new Error(String(res.status));
      const sample = await res.json();
      $("txtGateway").value = sample.gateway;
      $("txtMerchant").value = sample.merchant;
      document.querySelector('input[name="unit"][value="rupees"]').checked = true;
      updateUnitHelp();
      updateMeta("Gateway");
      updateMeta("Merchant");
      $("runStatus").textContent = "Sample loaded. Press Run to reconcile it.";
      $("run").focus();
    } catch (e) {
      showError("Could not load the sample data.", "Check your connection and try again.");
    } finally {
      btn.disabled = false;
    }
  });

  // ------------------------------------------------------------------- run
  function showError(message, detail, field) {
    const box = $("resError");
    box.replaceChildren(h("span", { text: message }), detail ? h("small", { text: detail }) : null);
    setView("error");
    $("runStatus").textContent = "Something needs attention.";
    if (field === "gateway" || field === "merchant") {
      const el = $("txt" + field[0].toUpperCase() + field.slice(1));
      el.setAttribute("aria-invalid", "true");
      el.focus();
    }
  }

  $("run").addEventListener("click", run);

  async function run() {
    const gateway = $("txtGateway").value;
    const merchant = $("txtMerchant").value;
    const body = JSON.stringify({ gateway, merchant, amount_unit: currentUnit() });
    if (bytesOf(body) > MAX_BODY_BYTES) {
      showError("That is more than 2 MB of data.", "Split the export into smaller date ranges and run each one.");
      return;
    }
    $("run").disabled = true;
    $("runStatus").textContent = "Reconciling…";
    setView("loading");
    let res, payload;
    try {
      res = await fetch("/api/reconcile", { method: "POST", headers: { "Content-Type": "application/json" }, body });
      payload = await res.json().catch(() => ({}));
    } catch (e) {
      showError("Could not reach the server.", "Check your connection and try again.");
      updateRunEnabled();
      return;
    }
    updateRunEnabled();
    if (!res.ok) {
      showError(payload.error || "The request was rejected (" + res.status + ").", null, payload.field);
      return;
    }
    state.result = payload;
    state.filter = "ALL";
    state.query = "";
    $("q").value = "";
    state.expanded.clear();
    render();
    const n = payload.summary.orders;
    const skipped = payload.input.invalid_rows;
    $("runStatus").textContent =
      "Done: " + n + " order" + (n === 1 ? "" : "s") + " reconciled" +
      (skipped ? ", " + skipped + " row" + (skipped === 1 ? "" : "s") + " skipped." : ".");
    const reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    $("results").scrollIntoView({ behavior: reduce ? "auto" : "smooth", block: "start" });
  }

  // ---------------------------------------------------------------- render
  function render() {
    const r = state.result;
    if (!r) { setView("empty"); return; }
    setView("body");
    renderCards(r);
    renderInvalid(r);
    renderChips(r);
    renderRows();
    renderActions(r);
  }

  function renderCards(r) {
    const cards = $("cards");
    cards.replaceChildren();
    const multiCurrency = r.orders.some((o) => o.currency && o.currency !== "INR");
    for (const v of VERDICTS) {
      const s = r.summary.by_verdict[v.key];
      cards.appendChild(
        h("div", { class: "vcard", "data-v": v.key },
          stamp(v.key),
          h("div", { class: "n", text: String(s.count) }),
          h("div", { class: "amt num", text: money(s.amount_paise) })));
    }
    cards.appendChild(
      h("div", { class: "vcard total" },
        h("span", { class: "stamp" }, "All orders"),
        h("div", { class: "n", text: String(r.summary.orders) }),
        h("div", { class: "amt num", text: money(r.summary.amount_paise) + (multiCurrency ? " (mixed currencies summed as given)" : "") })));
  }

  function renderInvalid(r) {
    const box = $("invalidBox");
    const bad = r.invalid_rows || [];
    box.hidden = bad.length === 0;
    if (!bad.length) return;
    $("invalidSummary").textContent =
      bad.length + " row" + (bad.length === 1 ? "" : "s") + " could not be read and " + (bad.length === 1 ? "was" : "were") + " skipped";
    $("invalidRows").replaceChildren(...bad.map((b) =>
      h("tr", null,
        h("td", { text: b.source === "gateway" ? "Gateway" : "Merchant" }),
        h("td", { class: "num", text: String(b.row) }),
        h("td", { class: "mono", text: b.order_id || "—" }),
        h("td", { text: b.reasons.join("; ") }))));
  }

  function renderChips(r) {
    const chips = $("chips");
    chips.replaceChildren();
    const make = (key, label, count) => {
      const b = h("button", { type: "button", class: "chip", "aria-pressed": state.filter === key ? "true" : "false" },
        label, h("span", { class: "c", text: "(" + count + ")" }));
      b.addEventListener("click", () => { state.filter = key; renderChips(r); renderRows(); });
      chips.appendChild(b);
    };
    make("ALL", "All", r.summary.orders);
    for (const v of VERDICTS) {
      const c = r.summary.by_verdict[v.key].count;
      if (c) make(v.key, v.label, c);
    }
  }

  function visibleOrders() {
    const r = state.result;
    const q = state.query.trim().toLowerCase();
    let rows = r.orders.filter((o) =>
      (state.filter === "ALL" || o.verdict === state.filter) &&
      (!q || o.order_id.toLowerCase().includes(q) || o.reason.toLowerCase().includes(q) ||
        (o.recommended_action || "").toLowerCase().includes(q)));
    const { key, dir } = state.sort;
    const val = (o) => {
      if (key === "verdict") return VERDICT_ORDER[o.verdict];
      if (key === "amount") return o.amount === null ? -1 : o.amount;
      return (o[key] || "").toString().toLowerCase();
    };
    rows = rows.map((o, i) => [o, i]).sort(([a, ia], [b, ib]) => {
      const x = val(a), y = val(b);
      const c = typeof x === "number" ? x - y : x.localeCompare(y);
      return (c || ia - ib) * dir;
    }).map(([o]) => o);
    return rows;
  }

  function renderRows() {
    const body = $("rows");
    const rows = visibleOrders();
    $("count").textContent = "Showing " + rows.length + " of " + state.result.orders.length + " orders";
    for (const th of document.querySelectorAll("th[data-sort]")) {
      th.setAttribute("aria-sort", th.dataset.sort === state.sort.key ? (state.sort.dir === 1 ? "ascending" : "descending") : "none");
    }
    if (!rows.length) {
      const td = h("td", { role: "cell", colspan: "6", text: "No orders match this filter." });
      td.style.padding = "22px 16px";
      td.style.color = "var(--muted)";
      body.replaceChildren(h("tr", { role: "row" }, td));
      return;
    }
    const frag = document.createDocumentFragment();
    rows.forEach((o, i) => {
      const trailId = "trail-" + i;
      const open = state.expanded.has(o.order_id);
      const btn = h("button", { type: "button", class: "expander", "aria-expanded": open ? "true" : "false", "aria-controls": trailId },
        h("span", { class: "oid", text: o.order_id }));
      const trailRow = h("tr", { class: "trailrow", id: trailId, role: "row" }, h("td", { role: "cell", colspan: "6" }, trailContent(o)));
      trailRow.hidden = !open;
      btn.addEventListener("click", () => {
        const now = btn.getAttribute("aria-expanded") !== "true";
        btn.setAttribute("aria-expanded", now ? "true" : "false");
        trailRow.hidden = !now;
        if (now) state.expanded.add(o.order_id); else state.expanded.delete(o.order_id);
      });
      frag.appendChild(
        h("tr", { class: "datarow", role: "row" },
          h("td", { class: "order", role: "cell" }, btn),
          h("td", { role: "cell", "data-label": "Verdict" }, stamp(o.verdict)),
          h("td", { class: "money num", role: "cell", "data-label": "Amount", text: money(o.amount, o.currency) }),
          h("td", { role: "cell", "data-label": "Gateway", text: o.gateway_status || "—" }),
          h("td", { role: "cell", "data-label": "Merchant", text: o.merchant_status || "—" }),
          h("td", { class: "reason", role: "cell", "data-label": "Why", text: o.reason })));
      frag.appendChild(trailRow);
    });
    body.replaceChildren(frag);
  }

  function trailContent(o) {
    return h("div", null,
      h("p", { class: "action-line", text: o.recommended_action }),
      h("ol", { class: "trail", "aria-label": "Decision trail for " + o.order_id },
        o.decision_trail.map((t) =>
          h("li", null, h("strong", { text: t.step }), h("span", { class: "act", text: t.action }), h("span", { class: "why", text: t.reason })))));
  }

  for (const th of document.querySelectorAll("th[data-sort]")) {
    th.querySelector("button").addEventListener("click", () => {
      const key = th.dataset.sort;
      state.sort = { key, dir: state.sort.key === key ? -state.sort.dir : 1 };
      const want = key + ":" + state.sort.dir;
      $("sortSel").value = Array.from($("sortSel").options).some((o) => o.value === want) ? want : $("sortSel").value;
      if (state.result) renderRows();
    });
  }
  $("sortSel").addEventListener("change", (ev) => {
    const [key, dir] = ev.target.value.split(":");
    state.sort = { key, dir: Number(dir) };
    if (state.result) renderRows();
  });
  $("q").addEventListener("input", (ev) => { state.query = ev.target.value; if (state.result) renderRows(); });

  // -------------------------------------------------------------- downloads
  function csvCell(v) {
    let s = v === null || v === undefined ? "" : String(v);
    // Keep spreadsheets from running a cell as a formula.
    if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
    return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }
  function toCsv(columns, rows) {
    return [columns.join(",")].concat(rows.map((r) => columns.map((c) => csvCell(r[c])).join(","))).join("\r\n") + "\r\n";
  }
  function download(name, text) {
    const url = URL.createObjectURL(new Blob([text], { type: "text/csv;charset=utf-8" }));
    const a = h("a", { href: url, download: name });
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  function stamp8() { return new Date().toISOString().slice(0, 10).replace(/-/g, ""); }
  function flat(o) {
    return {
      order_id: o.order_id, verdict: o.verdict, reason: o.reason,
      recommended_action: o.recommended_action, amount_paise: o.amount, amount_rupees: exactRupees(o.amount),
      currency: o.currency, gateway_status: o.gateway_status, merchant_status: o.merchant_status,
      decision_trail: o.decision_trail.map((t) => t.step + ": " + t.action).join(" | "),
    };
  }
  function corrections() { return state.result.orders.filter((o) => o.action_code === "mark_paid"); }
  function reviews() { return state.result.orders.filter((o) => REVIEW_VERDICTS.has(o.verdict)); }

  function renderActions(r) {
    const c = corrections().length, v = reviews().length;
    $("dlCorrections").disabled = c === 0;
    $("dlReview").disabled = v === 0;
    $("dlCorrections").textContent = "Download corrections CSV (" + c + ")";
    $("dlReview").textContent = "Download review CSV (" + v + ")";
    $("actionsWhy").textContent =
      "Corrections: " + c + " order" + (c === 1 ? "" : "s") + " where the merchant should be marked paid. " +
      "Review: " + v + " order" + (v === 1 ? "" : "s") + " that need a person. " +
      "These are recommendations; apply them in your own store. Files are made in your browser and nothing is sent anywhere.";
  }

  $("dlCorrections").addEventListener("click", () => {
    const cols = ["order_id", "recommended_action", "amount_paise", "amount_rupees", "currency", "gateway_status", "merchant_status", "reason"];
    download("verisync-corrections-" + stamp8() + ".csv",
      toCsv(cols, corrections().map((o) => ({ ...flat(o), recommended_action: "mark_paid" }))));
  });
  $("dlReview").addEventListener("click", () => {
    const cols = ["order_id", "verdict", "reason", "recommended_action", "amount_paise", "amount_rupees", "currency", "gateway_status", "merchant_status", "decision_trail"];
    download("verisync-review-" + stamp8() + ".csv", toCsv(cols, reviews().map(flat)));
  });

  updateUnitHelp();
  setView("empty");
})();
