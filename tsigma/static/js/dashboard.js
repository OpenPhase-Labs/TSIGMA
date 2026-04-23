/**
 * TSIGMA - Dashboard Page
 *
 * Initializes the map (MapLibre GL + OSM tiles), loads signal markers,
 * renders the event volume chart, and populates stat cards.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};

    var _map = null;
    var _volumeChart = null;
    var _refreshTimer = null;

    /**
     * Initialize the dashboard.  Call once when the page loads.
     */
    function initDashboard() {
        initMap();
        initVolumeChart();
        loadStats();
        loadSignals();

        // Auto-refresh markers every 60 s
        _refreshTimer = setInterval(loadSignals, 60000);
    }

    // ---------------------------------------------------------------
    // Map
    // ---------------------------------------------------------------

    function initMap() {
        var container = document.getElementById("dashboard-map");
        if (!container) return;

        _map = new maplibregl.Map({
            container: "dashboard-map",
            style: {
                version: 8,
                sources: {
                    osm: {
                        type: "raster",
                        tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
                        tileSize: 256,
                        attribution:
                            '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>',
                    },
                },
                layers: [
                    {
                        id: "osm-tiles",
                        type: "raster",
                        source: "osm",
                        minzoom: 0,
                        maxzoom: 19,
                    },
                ],
            },
            center: [-98.5, 39.8], // center of CONUS
            zoom: 4,
        });
    }

    function loadSignals() {
        tsigma.api
            .get("/api/v1/signals")
            .then(function (signals) {
                if (!_map || !Array.isArray(signals)) return;

                // Remove existing markers (stored on map instance)
                if (_map._tsigmaMarkers) {
                    _map._tsigmaMarkers.forEach(function (m) {
                        m.remove();
                    });
                }
                _map._tsigmaMarkers = [];

                var bounds = new maplibregl.LngLatBounds();
                var hasPoints = false;

                signals.forEach(function (sig) {
                    if (sig.latitude == null || sig.longitude == null) return;

                    hasPoints = true;
                    var color = sig.enabled ? "#22c55e" : "#9ca3af";

                    var el = document.createElement("div");
                    el.style.width = "14px";
                    el.style.height = "14px";
                    el.style.borderRadius = "50%";
                    el.style.backgroundColor = color;
                    el.style.border = "2px solid #fff";
                    el.style.boxShadow = "0 1px 3px rgba(0,0,0,0.3)";
                    el.style.cursor = "pointer";

                    var marker = new maplibregl.Marker({ element: el })
                        .setLngLat([sig.longitude, sig.latitude])
                        .setPopup(
                            new maplibregl.Popup({ offset: 12 }).setHTML(
                                "<strong>" +
                                    escapeHtml(sig.name || sig.signal_id) +
                                    "</strong><br/>" +
                                    (sig.main_street || "") +
                                    (sig.cross_street ? " & " + sig.cross_street : "")
                            )
                        )
                        .addTo(_map);

                    el.addEventListener("click", function () {
                        window.location.href = "/signals/" + sig.signal_id;
                    });

                    _map._tsigmaMarkers.push(marker);
                    bounds.extend([sig.longitude, sig.latitude]);
                });

                if (hasPoints) {
                    _map.fitBounds(bounds, { padding: 50, maxZoom: 14 });
                }
            })
            .catch(function () {
                // Error already surfaced by tsigma.api
            });
    }

    // ---------------------------------------------------------------
    // Event Volume Chart
    // ---------------------------------------------------------------

    function initVolumeChart() {
        var el = document.getElementById("volume-chart");
        if (!el) return;

        _volumeChart = echarts.init(el);
        tsigma.charts.resize(_volumeChart);
        tsigma.charts.loading("volume-chart", true);

        tsigma.api
            .get("/api/v1/events/volume")
            .then(function (data) {
                tsigma.charts.loading("volume-chart", false);
                renderVolumeChart(data);
            })
            .catch(function () {
                tsigma.charts.loading("volume-chart", false);
            });
    }

    function renderVolumeChart(data) {
        if (!_volumeChart || !data) return;

        var dates = data.map(function (d) { return d.date; });
        var counts = data.map(function (d) { return d.count; });

        _volumeChart.setOption({
            title: {
                text: "Event Volume (Last 7 Days)",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "axis",
                formatter: function (params) {
                    var p = params[0];
                    return p.axisValueLabel + "<br/>Events: " + p.value.toLocaleString();
                },
            },
            grid: { left: 60, right: 20, top: 40, bottom: 30 },
            xAxis: { type: "category", data: dates },
            yAxis: { type: "value", name: "Events" },
            series: [
                {
                    type: "bar",
                    data: counts,
                    itemStyle: { color: "#6366f1" },
                    barMaxWidth: 40,
                },
            ],
        });
    }

    // ---------------------------------------------------------------
    // Stat Cards
    // ---------------------------------------------------------------

    function loadStats() {
        tsigma.api
            .get("/api/v1/signals/summary")
            .then(function (summary) {
                setStatText("stat-total-signals", summary.total);
                setStatText("stat-enabled-signals", summary.enabled);
                setStatText("stat-events-today", summary.events_today);
            })
            .catch(function () {
                // Fail silently - cards keep placeholder text
            });
    }

    function setStatText(id, value) {
        var el = document.getElementById(id);
        if (el) {
            el.textContent = typeof value === "number" ? value.toLocaleString() : value;
        }
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
        document.addEventListener("DOMContentLoaded", initDashboard);
    } else {
        initDashboard();
    }
})();
