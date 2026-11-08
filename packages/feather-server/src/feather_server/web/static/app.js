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

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", filters);
  } else {
    filters();
  }
})();
