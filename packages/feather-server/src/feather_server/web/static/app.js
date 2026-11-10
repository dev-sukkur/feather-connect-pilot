/* Feather Connect dashboard: the little the pages do in the browser. Every page works without it: forms post, links
   go, and the server filters. With it come the command palette, the theme switch, the column chooser, sorting and
   moving through a table with the keyboard, ticking every row at once, opening a row, and confirming in a dialog. */
(function () {
  "use strict";

  var root = document.documentElement;
  var user = root.getAttribute("data-user") || "";

  function stored(key) {
    try { return window.localStorage.getItem(key + ":" + user); } catch (error) { return null; }
  }
  function store(key, value) {
    try {
      if (value === null) { window.localStorage.removeItem(key + ":" + user); }
      else { window.localStorage.setItem(key + ":" + user, value); }
    } catch (error) { /* a private window keeps nothing: the page still works */ }
  }
  function all(selector, within) { return Array.prototype.slice.call((within || document).querySelectorAll(selector)); }

  var announcer = null;
  function announce(text) {
    if (!announcer) {
      announcer = document.createElement("p");
      announcer.className = "sr-only";
      announcer.setAttribute("role", "status");
      document.body.appendChild(announcer);
    }
    announcer.textContent = "";
    window.setTimeout(function () { announcer.textContent = text; }, 30);
  }

  function typing(target) {
    return target && (target.isContentEditable || /^(INPUT|SELECT|TEXTAREA)$/.test(target.tagName));
  }

  /* A filter box hides the rows of a table that don't contain its text. It is hidden until this script shows it. */
  function filters() {
    all("input[data-filter]").forEach(function (box) {
      var holder = box.closest("[hidden]");
      if (holder) { holder.hidden = false; }
      var count = document.getElementById("record-count");
      box.addEventListener("input", function () {
        var wanted = box.value.trim().toLowerCase();
        var shown = 0;
        all(box.dataset.filter).forEach(function (row) {
          var match = !wanted || row.textContent.toLowerCase().indexOf(wanted) !== -1;
          row.hidden = !match;
          if (match) { shown += 1; }
        });
        if (count) { count.textContent = shown + (shown === 1 ? " record" : " records") + " shown on this page"; }
      });
    });
  }

  /* On a phone the sidebar's menu folds behind its Menu button. Without this script it stays open. */
  function sidebar() {
    var bar = document.getElementById("sidebar");
    var button = bar && bar.querySelector(".menu-toggle");
    if (!button) { return; }
    var narrow = window.matchMedia("(max-width: 640px)");
    function set(open) {
      bar.classList.toggle("collapsed", !open);
      button.setAttribute("aria-expanded", open ? "true" : "false");
    }
    button.hidden = false;
    set(!narrow.matches);
    button.addEventListener("click", function () { set(button.getAttribute("aria-expanded") !== "true"); });
    narrow.addEventListener("change", function (event) { set(!event.matches); });
    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && narrow.matches && button.getAttribute("aria-expanded") === "true") {
        set(false);
        button.focus();
      }
    });
  }

  /* The theme: the user's choice, kept in this browser, or the system's. theme.js applied it before the page drew. */
  function theme() {
    var toggle = document.getElementById("theme-toggle");
    var system = document.getElementById("theme-system");
    var light = window.matchMedia("(prefers-color-scheme: light)");
    function now() { return root.getAttribute("data-theme") || (light.matches ? "light" : "dark"); }
    function show() {
      var current = now();
      root.setAttribute("data-theme-now", current);
      if (toggle) {
        var other = current === "dark" ? "light" : "dark";
        toggle.querySelector(".sr-only").textContent = "Switch to the " + other + " theme";
        toggle.title = "Switch to the " + other + " theme";
      }
      if (system) { system.hidden = !root.hasAttribute("data-theme"); }
    }
    if (toggle) {
      toggle.hidden = false;
      toggle.addEventListener("click", function () {
        var next = now() === "dark" ? "light" : "dark";
        root.setAttribute("data-theme", next);
        store("fc-theme", next);
        show();
        announce("The " + next + " theme is on.");
      });
    }
    if (system) {
      system.addEventListener("click", function () {
        root.removeAttribute("data-theme");
        store("fc-theme", null);
        show();
        announce("The theme follows the system now.");
      });
    }
    light.addEventListener("change", show);
    show();
  }

  /* Dialogs keep the focus inside while open, and give it back to what opened them. */
  var opener = null;
  function openDialog(dialog, focus) {
    opener = document.activeElement;
    if (!dialog.open) { dialog.showModal(); }
    (focus || dialog.querySelector("input, button, [href], [tabindex]")).focus();
  }
  function trap(dialog) {
    dialog.addEventListener("keydown", function (event) {
      if (event.key !== "Tab") { return; }
      var inside = all("input:not([type=hidden]), button, [href], select, textarea, [tabindex]:not([tabindex='-1'])", dialog)
        .filter(function (el) { return !el.disabled && !el.closest("[hidden]") && el.offsetParent !== null; });
      if (!inside.length) { event.preventDefault(); return; }
      var first = inside[0], last = inside[inside.length - 1];
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first.focus(); }
    });
    dialog.addEventListener("close", function () {
      if (opener && document.contains(opener)) { opener.focus(); }
      opener = null;
    });
    dialog.addEventListener("click", function (event) { if (event.target === dialog) { dialog.close(); } });
  }

  /* The command palette: Ctrl+K or Cmd+K, or the search box in the top bar. It asks /search.json as you type. */
  function palette() {
    var dialog = document.getElementById("palette");
    var box = document.getElementById("global-search");
    if (!dialog || !box) { return; }
    var input = document.getElementById("palette-input");
    var list = document.getElementById("palette-list");
    var empty = document.getElementById("palette-empty");
    var said = document.getElementById("palette-said");
    var items = [], active = -1, timer = null, asked = 0;
    trap(dialog);

    function choose(index) {
      active = index;
      all("li", list).forEach(function (li, i) { li.setAttribute("aria-selected", i === index ? "true" : "false"); });
      if (index >= 0 && list.children[index]) {
        input.setAttribute("aria-activedescendant", list.children[index].id);
        list.children[index].scrollIntoView({ block: "nearest" });
      } else {
        input.removeAttribute("aria-activedescendant");
      }
    }
    function go(index) { if (items[index]) { window.location.assign(items[index].href); } }
    function draw(found) {
      items = found;
      list.textContent = "";
      found.forEach(function (item, i) {
        var li = document.createElement("li");
        li.id = "palette-option-" + i;
        li.setAttribute("role", "option");
        var label = document.createElement("span");
        label.textContent = item.label;
        var kind = document.createElement("span");
        kind.className = "kind";
        kind.textContent = item.detail || item.kind;
        li.appendChild(label);
        li.appendChild(kind);
        li.addEventListener("click", function () { go(i); });
        list.appendChild(li);
      });
      empty.hidden = found.length > 0;
      input.setAttribute("aria-expanded", found.length ? "true" : "false");
      if (said) { said.textContent = found.length === 1 ? "1 result" : found.length + " results"; }
      choose(found.length ? 0 : -1);
    }
    function ask() {
      var mine = ++asked;
      fetch("/search.json?q=" + encodeURIComponent(input.value.trim()), { credentials: "same-origin", headers: { Accept: "application/json" } })
        .then(function (response) { return response.ok ? response.json() : { items: [] }; })
        .then(function (body) { if (mine === asked) { draw(body.items || []); } })
        .catch(function () { if (mine === asked) { draw([]); } });
    }
    function open(text) {
      input.value = text || "";
      openDialog(dialog, input);
      ask();
    }
    input.addEventListener("input", function () {
      window.clearTimeout(timer);
      timer = window.setTimeout(ask, 120);
    });
    input.addEventListener("keydown", function (event) {
      if (event.key === "ArrowDown") { event.preventDefault(); choose(Math.min(active + 1, items.length - 1)); }
      else if (event.key === "ArrowUp") { event.preventDefault(); choose(Math.max(active - 1, 0)); }
      else if (event.key === "Home" && items.length) { event.preventDefault(); choose(0); }
      else if (event.key === "End" && items.length) { event.preventDefault(); choose(items.length - 1); }
      else if (event.key === "Enter") { event.preventDefault(); go(active); }
    });
    box.addEventListener("click", function () { open(box.value); });
    box.addEventListener("keydown", function (event) {
      if (event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
        event.preventDefault();
        open(box.value + event.key);
      }
    });
    document.addEventListener("keydown", function (event) {
      if ((event.ctrlKey || event.metaKey) && (event.key === "k" || event.key === "K")) {
        event.preventDefault();
        if (dialog.open) { dialog.close(); } else { open(""); }
      }
    });
  }

  /* The shortcuts: ? shows them, / goes to the list's search box. */
  function shortcuts() {
    var help = document.getElementById("help");
    var button = document.getElementById("help-open");
    if (help && button) {
      trap(help);
      button.hidden = false;
      button.addEventListener("click", function () { openDialog(help); });
    }
    document.addEventListener("keydown", function (event) {
      if (typing(event.target) || event.ctrlKey || event.metaKey || event.altKey || document.querySelector("dialog[open]")) { return; }
      if (event.key === "?" && help) { event.preventDefault(); openDialog(help); }
      if (event.key === "/") {
        var search = document.querySelector("[data-list-search]");
        if (search) { event.preventDefault(); search.focus(); search.select(); }
      }
    });
  }

  /* Confirming: a button or form with data-confirm asks in a dialog first. Without this script the server asks
     where it matters (cancelling runs has a second step of its own). */
  function confirming() {
    var dialog = document.getElementById("confirm-dialog");
    if (!dialog) { return; }
    trap(dialog);
    var text = document.getElementById("confirm-dialog-text");
    var yes = document.getElementById("confirm-dialog-yes");
    var no = document.getElementById("confirm-dialog-no");
    var pending = null;
    document.addEventListener("submit", function (event) {
      var form = event.target;
      var by = event.submitter;
      var question = (by && by.getAttribute("data-confirm")) || form.getAttribute("data-confirm");
      if (!question || form.dataset.confirmed === "yes") { return; }
      event.preventDefault();
      pending = { form: form, by: by };
      text.textContent = question;
      yes.textContent = (by && by.textContent.trim()) || "Confirm";
      openDialog(dialog, no);
    }, true);
    yes.addEventListener("click", function () {
      var job = pending;
      pending = null;
      dialog.close();
      if (!job) { return; }
      var field = job.by && job.by.getAttribute("data-confirm-field");
      if (field) {
        var hidden = job.form.querySelector("input[name='" + field + "']");
        if (!hidden) {
          hidden = document.createElement("input");
          hidden.type = "hidden";
          hidden.name = field;
          job.form.appendChild(hidden);
        }
        hidden.value = "yes";
      }
      job.form.dataset.confirmed = "yes";
      if (job.by) { job.form.requestSubmit(job.by); } else { job.form.requestSubmit(); }
      delete job.form.dataset.confirmed;
    });
    no.addEventListener("click", function () { pending = null; dialog.close(); });
  }

  /* The charts under the tiles: hidden until asked for, and remembered. */
  function charts() {
    var on = stored("fc-charts") === "on";
    root.classList.toggle("charts-on", on);
    all(".charts-toggle").forEach(function (button) {
      button.hidden = false;
      function show() {
        button.setAttribute("aria-pressed", on ? "true" : "false");
        button.lastChild.textContent = on ? " Hide charts" : " Show charts";
      }
      show();
      button.addEventListener("click", function () {
        on = !on;
        root.classList.toggle("charts-on", on);
        store("fc-charts", on ? "on" : null);
        show();
      });
    });
  }

  /* Tables: the chosen columns, sorting, ticking, opening a row, moving with the arrow keys, copying an id. */
  var sorted = {};

  function hiddenColumns(key) {
    var found = stored("fc-cols-" + key);
    return found ? found.split(",").filter(Boolean) : [];
  }
  function applyColumns(table, key) {
    var hidden = hiddenColumns(key);
    all("[data-col]", table).forEach(function (cell) { cell.hidden = hidden.indexOf(cell.getAttribute("data-col")) !== -1; });
  }
  function chooser(key) {
    all("details[data-columns-for='" + key + "']").forEach(function (details) {
      details.hidden = false;
      if (details.dataset.ready) { return; }
      details.dataset.ready = "1";
      var hidden = hiddenColumns(key);
      all("[data-col-toggle]", details).forEach(function (box) {
        box.checked = hidden.indexOf(box.getAttribute("data-col-toggle")) === -1;
        box.addEventListener("change", function () {
          var off = all("[data-col-toggle]", details)
            .filter(function (b) { return !b.checked; })
            .map(function (b) { return b.getAttribute("data-col-toggle"); });
          store("fc-cols-" + key, off.length ? off.join(",") : null);
          all("table[data-table='" + key + "']").forEach(function (table) { applyColumns(table, key); });
        });
      });
    });
  }

  function valueOf(cell) {
    if (!cell) { return ""; }
    var holder = cell.hasAttribute("data-value") ? cell : cell.querySelector("[data-value]");
    var own = holder ? holder.getAttribute("data-value") : null;
    if (own) { return isNaN(Number(own)) ? own.toLowerCase() : Number(own); }
    var time = cell.querySelector("time[datetime]");
    if (time) { return Date.parse(time.getAttribute("datetime")) || 0; }
    var text = cell.textContent.trim().toLowerCase();
    var number = Number(text.replace(/[,+]/g, ""));
    return text !== "" && !isNaN(number) ? number : text;
  }
  function sortBy(table, index, direction) {
    var body = table.tBodies[0];
    var groups = [];
    all(":scope > tr", body).forEach(function (row) {
      if (row.classList.contains("child") && groups.length) { groups[groups.length - 1].push(row); }
      else { groups.push([row]); }
    });
    groups.sort(function (a, b) {
      var x = valueOf(a[0].children[index]), y = valueOf(b[0].children[index]);
      if (x === y) { return 0; }
      return (x > y ? 1 : -1) * direction;
    });
    groups.forEach(function (group) { group.forEach(function (row) { body.appendChild(row); }); });
    all("thead th", table).forEach(function (th, i) {
      if (th.hasAttribute("data-sort")) {
        if (i === index) { th.setAttribute("aria-sort", direction > 0 ? "ascending" : "descending"); }
        else { th.removeAttribute("aria-sort"); }
      }
    });
  }
  function sorting(table, key) {
    all("thead th[data-sort]", table).forEach(function (th) {
      if (th.querySelector(".sort")) { return; }
      var button = document.createElement("button");
      button.type = "button";
      button.className = "sort";
      while (th.firstChild) { button.appendChild(th.firstChild); }
      th.appendChild(button);
      button.addEventListener("click", function () {
        var index = Array.prototype.indexOf.call(th.parentNode.children, th);
        var direction = th.getAttribute("aria-sort") === "ascending" ? -1 : 1;
        sorted[key] = [index, direction];
        sortBy(table, index, direction);
      });
    });
    if (sorted[key]) { sortBy(table, sorted[key][0], sorted[key][1]); }
  }

  function bulkCount(table) {
    var ticked = all("[data-select]", table).filter(function (b) { return b.checked; }).length;
    var owner = all("[data-select]", table)[0];
    var form = owner && owner.form;
    var every = table.querySelector("[data-select-all]");
    var boxes = all("[data-select]", table);
    if (every) {
      every.checked = boxes.length > 0 && ticked === boxes.length;
      every.indeterminate = ticked > 0 && ticked < boxes.length;
    }
    boxes.forEach(function (b) { var row = b.closest("tr"); if (row) { row.classList.toggle("is-selected", b.checked); } });
    if (form && form.hasAttribute("data-bulk")) {
      form.classList.toggle("has-selection", ticked > 0);
      var count = form.querySelector("[data-bulk-count]");
      if (count) { count.textContent = ticked ? ticked + " ticked" : "Tick rows to act on several at once."; }
      all("button[name=action]", form).forEach(function (b) { b.disabled = ticked === 0; });
    }
  }
  function ticking(table) {
    var every = table.querySelector("[data-select-all]");
    if (every) {
      every.hidden = false;
      every.addEventListener("change", function () {
        all("[data-select]", table).forEach(function (b) { b.checked = every.checked; });
        bulkCount(table);
      });
    }
    all("[data-select]", table).forEach(function (b) { b.addEventListener("change", function () { bulkCount(table); }); });
    bulkCount(table);
  }

  function opening(table) {
    all("button.expand[data-child-src]", table).forEach(function (button) {
      button.hidden = false;
      button.addEventListener("click", function () {
        var child = document.getElementById(button.getAttribute("aria-controls"));
        if (!child) { return; }
        var open = button.getAttribute("aria-expanded") !== "true";
        button.setAttribute("aria-expanded", open ? "true" : "false");
        child.hidden = !open;
        var body = child.querySelector(".child-body");
        if (open && body && !body.dataset.loaded) {
          body.dataset.loaded = "1";
          body.textContent = "Loading…";
          fetch(button.getAttribute("data-child-src"), { credentials: "same-origin" })
            .then(function (response) {
              // The session ran out: the fetch followed the redirect to the sign-in page, or was refused.
              if (response.status === 401 || (response.redirected && new URL(response.url).pathname === "/login")) {
                window.location.assign("/login");
                return Promise.reject(401);
              }
              return response.ok ? response.text() : Promise.reject(response.status);
            })
            .then(function (html) { body.innerHTML = html; })
            .catch(function () { body.textContent = "This could not be loaded. Open the row's page instead."; delete body.dataset.loaded; });
        }
      });
    });
  }

  function moving(table) {
    table.addEventListener("keydown", function (event) {
      if (["ArrowDown", "ArrowUp", "Home", "End"].indexOf(event.key) === -1) { return; }
      if (event.target.tagName === "SELECT" || (event.target.tagName === "INPUT" && event.target.type !== "checkbox")) { return; }
      var row = event.target.closest("tbody > tr:not(.child)");
      if (!row || !table.contains(row)) { return; }
      var rows = all(":scope > tbody > tr:not(.child)", table).filter(function (r) { return !r.hidden; });
      var at = rows.indexOf(row);
      var to = event.key === "ArrowDown" ? at + 1 : event.key === "ArrowUp" ? at - 1 : event.key === "Home" ? 0 : rows.length - 1;
      if (to < 0 || to >= rows.length || to === at) { return; }
      event.preventDefault();
      var target = rows[to].querySelector("th a") || rows[to].querySelector("a, button:not([hidden]), input");
      if (target) { target.focus(); }
    });
  }

  function copying(within) {
    all("[data-copy]", within).forEach(function (button) {
      if (!navigator.clipboard || button.dataset.ready) { return; }
      button.dataset.ready = "1";
      button.hidden = false;
      button.addEventListener("click", function () {
        navigator.clipboard.writeText(button.getAttribute("data-copy")).then(
          function () { announce("Copied " + button.getAttribute("data-copy") + "."); },
          function () { announce("The browser did not allow copying."); }
        );
        var menu = button.closest("details");
        if (menu) { menu.open = false; }
      });
    });
  }

  /* A row's More menu opens over the table, not inside its scroll box, and closes on Escape or a click away. */
  function menus() {
    var openedAt = 0;  // when a row menu was last asked open: its toggle comes after the scroll that showing it makes
    document.addEventListener("pointerdown", function (event) {
      if (event.target.closest && event.target.closest("details.row-menu > summary")) { openedAt = Date.now(); }
    }, true);
    document.addEventListener("keydown", function (event) {
      if ((event.key === "Enter" || event.key === " ") && event.target.matches && event.target.matches("details.row-menu > summary")) {
        openedAt = Date.now();
      }
    }, true);
    document.addEventListener("toggle", function (event) {
      var details = event.target;
      if (!details.classList || !details.open) { return; }
      if (!details.matches(".row-menu, .columns, .user-menu")) { return; }
      all("details.row-menu[open], details.columns[open], details.user-menu[open]").forEach(function (other) {
        if (other !== details) { other.open = false; }
      });
      if (details.classList.contains("row-menu")) {
        openedAt = Date.now();
        var panel = details.querySelector(".menu-panel");
        var spot = details.querySelector("summary").getBoundingClientRect();
        panel.style.position = "fixed";
        panel.style.top = Math.min(spot.bottom + 4, window.innerHeight - panel.offsetHeight - 8) + "px";
        panel.style.left = Math.max(8, spot.right - panel.offsetWidth) + "px";
      }
    }, true);
    document.addEventListener("keydown", function (event) {
      if (event.key !== "Escape") { return; }
      all("details.row-menu[open], details.columns[open], details.user-menu[open]").forEach(function (details) {
        details.open = false;
        details.querySelector("summary").focus();
      });
    });
    document.addEventListener("click", function (event) {
      all("details.row-menu[open], details.columns[open], details.user-menu[open]").forEach(function (details) {
        if (!details.contains(event.target)) { details.open = false; }
      });
    });
    window.addEventListener("scroll", function () {
      // Opening a menu can scroll its table to show it: that scroll is not the reader's, and must not close it.
      if (Date.now() - openedAt < 400) { return; }
      all("details.row-menu[open]").forEach(function (details) { details.open = false; });
    }, true);
  }

  function tables(within) {
    all("table[data-table]", within).forEach(function (table) {
      if (table.dataset.enhanced) { return; }
      table.dataset.enhanced = "1";
      var key = table.getAttribute("data-table");
      chooser(key);
      applyColumns(table, key);
      sorting(table, key);
      ticking(table);
      opening(table);
      moving(table);
    });
    copying(within);
  }

  /* A list refreshes itself, but not while a row of it is ticked, open, or has the focus: that would undo it. */
  function live() {
    document.body.addEventListener("htmx:beforeRequest", function (event) {
      var source = event.detail && event.detail.elt;
      if (!source || source.id !== "live") { return; }
      var busy = source.querySelector("[data-select]:checked, details[open], button.expand[aria-expanded='true']") ||
        source.contains(document.activeElement) && document.activeElement !== document.body;
      all(".live-note", source).forEach(function (note) { note.hidden = !busy; });
      if (busy) { event.preventDefault(); }
    });
    document.body.addEventListener("htmx:afterSettle", function () {
      tables(document);
      all("form[data-bulk]").forEach(function (form) {
        var first = document.querySelector("[data-select][form='" + form.id + "']");
        if (first) { bulkCount(first.closest("table")); }
        else {
          form.classList.remove("has-selection");
          all("button[name=action]", form).forEach(function (b) { b.disabled = true; });
        }
      });
    });
  }

  function start() {
    filters();
    sidebar();
    theme();
    palette();
    shortcuts();
    confirming();
    charts();
    menus();
    tables(document);
    live();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
