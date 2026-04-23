/**
 * TSIGMA Common Utilities
 *
 * Shared helpers used across all pages: API fetch wrappers,
 * date formatting, and ECharts lifecycle utilities.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};

    // ---------------------------------------------------------------
    // API helpers
    // ---------------------------------------------------------------
    var api = {};

    /**
     * Wrapper around fetch() that handles JSON parsing, credentials,
     * 401 redirects, and error display.
     *
     * @param {string} url
     * @param {RequestInit} [options]
     * @returns {Promise<any>} parsed JSON body
     */
    api.fetch = function (url, options) {
        var opts = Object.assign({ credentials: "same-origin" }, options || {});

        return fetch(url, opts)
            .then(function (res) {
                if (res.status === 401) {
                    window.location.href = "/login";
                    return Promise.reject(new Error("Unauthorized"));
                }
                if (!res.ok) {
                    return res.text().then(function (body) {
                        var msg = "API error " + res.status;
                        try {
                            var parsed = JSON.parse(body);
                            if (parsed.detail) msg = parsed.detail;
                            else if (parsed.message) msg = parsed.message;
                        } catch (_) {
                            if (body) msg = body;
                        }
                        throw new Error(msg);
                    });
                }
                return res.json();
            })
            .catch(function (err) {
                tsigma.notify(err.message || "Network error", "error");
                throw err;
            });
    };

    /**
     * Convenience GET request.
     * @param {string} url
     * @returns {Promise<any>}
     */
    api.get = function (url) {
        return api.fetch(url, { method: "GET" });
    };

    /**
     * Convenience POST request with JSON body.
     * @param {string} url
     * @param {any} body
     * @returns {Promise<any>}
     */
    api.post = function (url, body) {
        return api.fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
    };

    window.tsigma.api = api;

    // ---------------------------------------------------------------
    // Date helpers
    // ---------------------------------------------------------------
    var dates = {};

    /**
     * Format a Date object to an ISO-8601 string (UTC).
     * @param {Date} date
     * @returns {string}
     */
    dates.formatISO = function (date) {
        return date.toISOString();
    };

    /**
     * Format an ISO-8601 string for human-friendly display.
     * Output: "Mar 22, 2026 14:05"
     * @param {string} isoString
     * @returns {string}
     */
    dates.formatDisplay = function (isoString) {
        var d = new Date(isoString);
        var months = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
        ];
        var mon = months[d.getMonth()];
        var day = d.getDate();
        var year = d.getFullYear();
        var hh = String(d.getHours()).padStart(2, "0");
        var mm = String(d.getMinutes()).padStart(2, "0");
        return mon + " " + day + ", " + year + " " + hh + ":" + mm;
    };

    window.tsigma.dates = dates;

    // ---------------------------------------------------------------
    // Chart helpers
    // ---------------------------------------------------------------
    var charts = window.tsigma.charts || {};

    /**
     * Attach a debounced resize listener so the chart reflows when
     * the browser window changes size.
     * @param {echarts.ECharts} chart
     */
    charts.resize = function (chart) {
        var timer = null;
        window.addEventListener("resize", function () {
            clearTimeout(timer);
            timer = setTimeout(function () {
                chart.resize();
            }, 150);
        });
    };

    /**
     * Show or hide a loading overlay on a chart container.
     * When showing, it displays an animated spinner; when hiding,
     * it removes the overlay.
     *
     * @param {string} chartId  DOM id of the chart container
     * @param {boolean} show
     */
    charts.loading = function (chartId, show) {
        var container = document.getElementById(chartId);
        if (!container) return;

        var overlay = container.querySelector(".tsigma-loading");

        if (show) {
            if (overlay) return; // already visible
            overlay = document.createElement("div");
            overlay.className = "tsigma-loading";
            overlay.innerHTML =
                '<div class="tsigma-spinner"></div>' +
                '<span class="text-sm text-gray-500 mt-2">Loading&hellip;</span>';
            container.style.position = container.style.position || "relative";
            container.appendChild(overlay);
        } else {
            if (overlay) overlay.remove();
        }
    };

    window.tsigma.charts = charts;

    // ---------------------------------------------------------------
    // Toast / notification helper
    // ---------------------------------------------------------------

    /**
     * Display a brief notification toast.
     * @param {string} message
     * @param {"info"|"error"|"success"} [level="info"]
     */
    window.tsigma.notify = function (message, level) {
        level = level || "info";

        var colors = {
            info: "bg-blue-600",
            error: "bg-red-600",
            success: "bg-emerald-600",
        };

        var container = document.getElementById("tsigma-toasts");
        if (!container) {
            container = document.createElement("div");
            container.id = "tsigma-toasts";
            container.className =
                "fixed top-4 right-4 z-50 flex flex-col gap-2 pointer-events-none";
            document.body.appendChild(container);
        }

        var toast = document.createElement("div");
        toast.className =
            "pointer-events-auto px-4 py-2 rounded shadow-lg text-white text-sm " +
            (colors[level] || colors.info) +
            " transition-opacity duration-300 opacity-0";
        toast.textContent = message;
        container.appendChild(toast);

        // Animate in
        requestAnimationFrame(function () {
            toast.classList.remove("opacity-0");
            toast.classList.add("opacity-100");
        });

        // Auto-dismiss
        setTimeout(function () {
            toast.classList.remove("opacity-100");
            toast.classList.add("opacity-0");
            setTimeout(function () {
                toast.remove();
            }, 300);
        }, 5000);
    };
})();
