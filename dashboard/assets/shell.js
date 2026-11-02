/* Tabs: accessible tablist with roving tabindex, arrow/Home/End keys, #hash. */
(function () {
  "use strict";
  const tabs = Array.from(document.querySelectorAll('[role="tab"]'));
  const listeners = [];
  let current = "reconcile";

  function nameOf(tab) { return tab.id.replace(/^tab-/, ""); }

  function select(name, focus) {
    const target = tabs.find((t) => nameOf(t) === name) || tabs[0];
    current = nameOf(target);
    for (const t of tabs) {
      const on = t === target;
      t.setAttribute("aria-selected", on ? "true" : "false");
      t.tabIndex = on ? 0 : -1;
      const panel = document.getElementById(t.getAttribute("aria-controls"));
      if (panel) panel.hidden = !on;
    }
    if (focus) target.focus();
    try { history.replaceState(null, "", "#" + current); } catch (e) { /* file:// etc. */ }
    for (const fn of listeners) fn(current);
  }

  for (const t of tabs) {
    t.addEventListener("click", () => select(nameOf(t), false));
    t.addEventListener("keydown", (ev) => {
      const i = tabs.indexOf(t);
      let j = null;
      if (ev.key === "ArrowRight") j = (i + 1) % tabs.length;
      else if (ev.key === "ArrowLeft") j = (i - 1 + tabs.length) % tabs.length;
      else if (ev.key === "Home") j = 0;
      else if (ev.key === "End") j = tabs.length - 1;
      if (j !== null) { ev.preventDefault(); select(nameOf(tabs[j]), true); }
    });
  }

  window.verisync = {
    activeTab: () => current,
    onTab: (fn) => { listeners.push(fn); },
    select,
  };

  const initial = (location.hash || "").replace("#", "");
  select(tabs.some((t) => nameOf(t) === initial) ? initial : "reconcile", false);
})();
