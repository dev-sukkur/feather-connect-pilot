/* Feather Connect dashboard: the little the pages do in the browser. Every page works without it. */
(function () {
  "use strict";

  /* A filter box hides the rows of a table that don't contain its text. It is hidden until this script shows it. */
  function filters() {
    document.querySelectorAll("input[data-filter]").forEach(function (box) {
      var holder = box.closest("[hidden]");
      if (holder) { holder.hidden = false; }
      var count = document.getElementById("record-count");
      box.addEventListener("input", function () {
        var wanted = box.value.trim().toLowerCase();
        var shown = 0;
        document.querySelectorAll(box.dataset.filter).forEach(function (row) {
          var match = !wanted || row.textContent.toLowerCase().indexOf(wanted) !== -1;
          row.hidden = !match;
          if (match) { shown += 1; }
        });
        if (count) { count.textContent = shown + (shown === 1 ? " record" : " records") + " shown on this page"; }
      });
    });
  }

  /* Under 900px the sidebar's menu folds behind its Menu button. Without this script it stays open. */
  function sidebar() {
    var bar = document.getElementById("sidebar");
    var button = bar && bar.querySelector(".menu-toggle");
    if (!button) { return; }
    var narrow = window.matchMedia("(max-width: 900px)");
    function set(open) {
      bar.classList.toggle("collapsed", !open);
      button.setAttribute("aria-expanded", open ? "true" : "false");
    }
    button.hidden = false;
    set(!narrow.matches);
    button.addEventListener("click", function () {
      set(button.getAttribute("aria-expanded") !== "true");
    });
    narrow.addEventListener("change", function (event) { set(!event.matches); });
    document.addEventListener("keydown", function (event) {
      if (event.key === "Escape" && narrow.matches && button.getAttribute("aria-expanded") === "true") {
        set(false);
        button.focus();
      }
    });
  }

  function start() {
    filters();
    sidebar();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", start);
  } else {
    start();
  }
})();
