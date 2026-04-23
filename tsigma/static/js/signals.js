/**
 * TSIGMA - Signal List Page
 *
 * Fetches signals with filters, renders signal cards, and handles
 * search / filter interactions.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};

    var _debounceTimer = null;

    /**
     * Initialize the signals list page.
     */
    function initSignals() {
        loadFilters();
        loadSignals();
        bindEvents();
    }

    // ---------------------------------------------------------------
    // Filters
    // ---------------------------------------------------------------

    function loadFilters() {
        tsigma.api
            .get("/api/v1/signals/filters")
            .then(function (filters) {
                populateSelect("filter-jurisdiction", filters.jurisdictions);
                populateSelect("filter-region", filters.regions);
                populateSelect("filter-corridor", filters.corridors);
            })
            .catch(function () {
                // Dropdowns stay empty - not fatal
            });
    }

    function populateSelect(id, items) {
        var select = document.getElementById(id);
        if (!select || !Array.isArray(items)) return;

        // Keep the first "All" option
        while (select.options.length > 1) {
            select.remove(1);
        }

        items.forEach(function (item) {
            var opt = document.createElement("option");
            opt.value = item;
            opt.textContent = item;
            select.appendChild(opt);
        });
    }

    function getActiveFilters() {
        var params = new URLSearchParams();

        var search = document.getElementById("signal-search");
        if (search && search.value.trim()) {
            params.set("search", search.value.trim());
        }

        ["jurisdiction", "region", "corridor"].forEach(function (key) {
            var sel = document.getElementById("filter-" + key);
            if (sel && sel.value) {
                params.set(key, sel.value);
            }
        });

        return params.toString();
    }

    // ---------------------------------------------------------------
    // Signal List
    // ---------------------------------------------------------------

    function loadSignals() {
        var qs = getActiveFilters();
        var url = "/api/v1/signals" + (qs ? "?" + qs : "");

        var grid = document.getElementById("signal-grid");
        if (grid) {
            grid.innerHTML =
                '<div class="col-span-full text-center py-8 text-gray-400">' +
                '<div class="tsigma-spinner mx-auto mb-2"></div>Loading signals&hellip;</div>';
        }

        tsigma.api
            .get(url)
            .then(function (signals) {
                renderSignals(signals);
            })
            .catch(function () {
                if (grid) {
                    grid.innerHTML =
                        '<div class="col-span-full text-center py-8 text-red-400">Failed to load signals.</div>';
                }
            });
    }

    function renderSignals(signals) {
        var grid = document.getElementById("signal-grid");
        if (!grid) return;

        if (!signals || signals.length === 0) {
            grid.innerHTML =
                '<div class="col-span-full text-center py-8 text-gray-400">No signals found.</div>';
            return;
        }

        grid.innerHTML = "";

        signals.forEach(function (sig) {
            var card = document.createElement("a");
            card.href = "/signals/" + sig.signal_id;
            card.className =
                "block bg-white rounded-lg shadow-sm border border-gray-200 p-4 " +
                "hover:shadow-md hover:border-indigo-300 transition-all duration-200";

            var statusDot = sig.enabled
                ? '<span class="inline-block w-2.5 h-2.5 rounded-full bg-green-500 mr-2"></span>'
                : '<span class="inline-block w-2.5 h-2.5 rounded-full bg-gray-400 mr-2"></span>';

            var name = escapeHtml(sig.name || sig.signal_id);
            var streets = "";
            if (sig.main_street) {
                streets = escapeHtml(sig.main_street);
                if (sig.cross_street) streets += " &amp; " + escapeHtml(sig.cross_street);
            }

            var meta = [];
            if (sig.jurisdiction) meta.push(escapeHtml(sig.jurisdiction));
            if (sig.region) meta.push(escapeHtml(sig.region));

            card.innerHTML =
                '<div class="flex items-center mb-1">' +
                statusDot +
                '<span class="font-semibold text-gray-800">' + name + "</span>" +
                "</div>" +
                (streets
                    ? '<p class="text-sm text-gray-600 mb-1">' + streets + "</p>"
                    : "") +
                (meta.length
                    ? '<p class="text-xs text-gray-400">' + meta.join(" / ") + "</p>"
                    : "");

            grid.appendChild(card);
        });
    }

    // ---------------------------------------------------------------
    // Event Binding
    // ---------------------------------------------------------------

    function bindEvents() {
        var search = document.getElementById("signal-search");
        if (search) {
            search.addEventListener("input", function () {
                clearTimeout(_debounceTimer);
                _debounceTimer = setTimeout(loadSignals, 300);
            });
        }

        ["filter-jurisdiction", "filter-region", "filter-corridor"].forEach(function (id) {
            var el = document.getElementById(id);
            if (el) {
                el.addEventListener("change", loadSignals);
            }
        });
    }

    // ---------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------

    function escapeHtml(str) {
        var div = document.createElement("div");
        div.appendChild(document.createTextNode(str));
        return div.innerHTML;
    }

    // ---------------------------------------------------------------
    // Boot
    // ---------------------------------------------------------------

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", initSignals);
    } else {
        initSignals();
    }
})();
