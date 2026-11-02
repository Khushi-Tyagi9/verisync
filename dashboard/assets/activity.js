/* System activity: the pipeline's counters, review queue and decision feed, in
   plain language. Raw ids and codes sit behind "Show technical details". Polls
   only while its tab is open. */
(function () {
  "use strict";
  const POLL_MS = Number(document.body.dataset.pollMs) || 2000;
  const $ = (id) => document.getElementById(id);

  const DECISIONS = {
    ARM_RECHECK: "Waiting to recheck",
    APPLY_AUTOCORRECT: "Correction recommended",
    MARK_RESOLVED_NATURALLY: "Merchant caught up on its own",
    FLAG_FOR_REVIEW: "Sent to review",
    ROUTE_DISPUTED: "Refund or dispute",
    RECHECK_RETRIES_EXHAUSTED: "Gave up after retries",
    NO_OP: "Nothing to do",
    LOCKED_REPLAY_IGNORED: "Ignored (already settled)",
    RECLAIM: "Recovered a stuck check",
    DEAD_LETTER: "Gave up, needs a person",
  };
  const STATES = {
    PENDING_RECHECK: "waiting to recheck",
    PROCESSING: "rechecking",
    RESOLVED_AUTOCORRECTED: "correction recommended",
    RESOLVED_NATURALLY: "merchant caught up",
    DISPUTED: "refund or dispute",
    DEAD_LETTER: "needs a person",
  };
  const SOURCES = { razorpay: "Gateway", merchant: "Merchant", system: "verisync" };
  const REASONS = {
    unsafe_drift: "Merchant says paid, gateway says failed",
    amount_mismatch: "Amounts disagree",
    currency_mismatch: "Currencies disagree",
    no_merchant_record: "No merchant record",
    dead_letter: "Gave up after retries",
    missing_order_id: "Missing order id",
    unusable_envelope: "Unreadable message",
    undeserializable_envelope: "Unreadable message",
  };
  const plain = (map, k) => map[k] || (k ? String(k).toLowerCase().replace(/_/g, " ") : "");

  function el(tag, cls, text) {
    const e = document.createElement(tag);
    if (cls) e.className = cls;
    if (text != null) e.textContent = text;
    return e;
  }
  function fmtTime(iso) {
    if (!iso) return "";
    return new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  }

  function renderCards(o) {
    // Same single row of numbers as the reconcile summary.
    const defs = [
      [null, o.tracked, "Orders seen"],
      ["AUTO_CORRECT", o.auto_corrected, "Corrections recommended"],
      ["CONSISTENT", o.resolved_naturally, "Merchant caught up"],
      ["DISPUTED", o.disputed, "Refunds or disputes"],
      ["FLAGGED_RISKY", o.dead_letter, "Need a person"],
      ["UNRESOLVED", o.in_progress, "Waiting to recheck"],
    ];
    const row = $("actCards");
    row.replaceChildren();
    for (const [v, n, label] of defs) {
      const item = el("div", "item");
      if (v) item.dataset.v = v;
      const dt = el("dt");
      if (v) { const d = el("span", "dot"); d.setAttribute("aria-hidden", "true"); dt.appendChild(d); }
      dt.appendChild(document.createTextNode(label));
      item.appendChild(dt);
      item.appendChild(el("dd", null, String(n ?? 0)));
      row.appendChild(item);
    }
  }

  function renderReview(rq) {
    $("rqOpen").textContent = rq.open ?? 0;
    const chips = $("rqChips");
    chips.replaceChildren();
    for (const [reason, n] of Object.entries(rq.by_reason || {})) {
      chips.appendChild(el("span", "pill t-quiet", plain(REASONS, reason) + " · " + n));
    }
    const list = $("rqList");
    list.replaceChildren();
    for (const item of rq.recent || []) {
      const li = el("li");
      li.appendChild(el("span", "oid", item.order_id || "(no order id)"));
      li.appendChild(document.createTextNode(" "));
      li.appendChild(el("span", "rsn", plain(REASONS, item.reason)));
      li.appendChild(el("div", "sub", fmtTime(item.at)));
      if (item.event_id) li.appendChild(el("div", "tech", "event " + item.event_id + " · reason code " + item.reason));
      list.appendChild(li);
    }
    if (!(rq.recent || []).length) list.appendChild(el("li", "sub", "Nothing needs attention."));
  }

  const seen = new Set();
  let firstLoad = true;
  function renderFeed(rows) {
    const body = $("feed");
    body.replaceChildren();
    if (!rows.length) {
      const tr = el("tr");
      const td = el("td", "sub", "No decisions yet. Submit an order or run the pipeline to see activity here.");
      td.colSpan = 5;
      tr.appendChild(td);
      body.appendChild(tr);
    }
    for (const a of rows) {
      const key = (a.at || "") + "|" + (a.order_id || "") + "|" + (a.decision || "") + "|" + (a.new_state || "");
      const fresh = !firstLoad && !seen.has(key);
      seen.add(key);
      const tr = el("tr", fresh ? "fresh" : null);
      tr.appendChild(el("td", "sub", fmtTime(a.at)));
      const oid = el("td", "oid", a.order_id || "—");
      tr.appendChild(oid);
      tr.appendChild(el("td", null, plain(SOURCES, a.source)));
      const what = el("td");
      what.appendChild(el("span", null, plain(DECISIONS, a.decision) || "—"));
      what.appendChild(el("div", "tech", (a.decision || "") + (a.event_type ? " · " + a.event_type : "")));
      tr.appendChild(what);
      const moved = a.old_state && a.old_state !== a.new_state
        ? plain(STATES, a.old_state) + " → " + plain(STATES, a.new_state)
        : plain(STATES, a.new_state || a.old_state);
      const change = el("td", "sub", moved);
      change.appendChild(el("div", "tech", [a.old_state, a.new_state].filter(Boolean).join(" -> ")));
      tr.appendChild(change);
      body.appendChild(tr);
    }
    firstLoad = false;
  }

  function setLive(ok, detail) {
    $("dot").className = "dot " + (ok ? "live" : "stale");
    $("livetext").textContent = ok ? "live · updated " + new Date().toLocaleTimeString() : "not connected";
    const box = $("actError");
    box.hidden = ok;
    if (!ok) {
      const body = el("div");
      body.appendChild(el("span", "msg", "System activity is unavailable."));
      body.appendChild(el("small", null, detail || "The server could not read the database. The reconcile tool still works without it."));
      box.replaceChildren(body);
    }
  }

  async function tick() {
    if (window.verisync.activeTab() !== "activity" || document.hidden) return;
    try {
      const res = await fetch("/api/summary", { cache: "no-store" });
      if (!res.ok) throw new Error(String(res.status));
      const s = await res.json();
      renderCards(s.orders);
      const orphans = s.orders.orphan_pointers || 0;
      const warn = $("orphanWarn");
      warn.hidden = orphans === 0;
      if (orphans) {
        warn.textContent = orphans + " order record(s) are missing their history. Something wrote state outside the normal path; this should be 0.";
      }
      renderReview(s.review_queue);
      $("oos").textContent = s.out_of_scope.total ?? 0;
      renderFeed(s.activity || []);
      setLive(true);
    } catch (e) {
      setLive(false);
    }
  }

  $("techToggle").addEventListener("change", (ev) => {
    $("panel-activity").classList.toggle("show-tech", ev.target.checked);
  });
  window.verisync.onTab((name) => { if (name === "activity") tick(); });
  setInterval(tick, POLL_MS);
  tick();
})();
