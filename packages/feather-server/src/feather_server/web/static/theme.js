/* Feather Connect dashboard: the theme, set before the page is drawn so that it never flashes the other one.
   The choice is kept per user in this browser; with none, the page follows the system setting. */
(function () {
  "use strict";
  var root = document.documentElement;
  var key = "fc-theme:" + (root.getAttribute("data-user") || "");
  var chosen = null;
  try { chosen = window.localStorage.getItem(key); } catch (error) { chosen = null; }
  if (chosen === "light" || chosen === "dark") { root.setAttribute("data-theme", chosen); }
})();
