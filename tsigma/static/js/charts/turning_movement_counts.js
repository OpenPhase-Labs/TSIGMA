/**
 * TSIGMA - Turning Movement Counts Chart
 *
 * Stacked bar chart of vehicle volumes per direction over time bins.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var turningMovementCounts = {};
    var _chart = null;

    var DIRECTION_COLORS = {
        NB: "#3b82f6",
        SB: "#ef4444",
        EB: "#22c55e",
        WB: "#f59e0b",
        NBL: "#2563eb",
        NBT: "#3b82f6",
        NBR: "#60a5fa",
        SBL: "#dc2626",
        SBT: "#ef4444",
        SBR: "#f87171",
        EBL: "#16a34a",
        EBT: "#22c55e",
        EBR: "#4ade80",
        WBL: "#d97706",
        WBT: "#f59e0b",
        WBR: "#fbbf24",
    };

    turningMovementCounts.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Turning movement counts container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    turningMovementCounts.render = function (data) {
        if (!_chart) throw new Error("Turning movement counts chart not initialized");
        if (!data || data.length === 0) {
            _chart.clear();
            _chart.setOption({
                title: {
                    text: "No data available",
                    left: "center",
                    top: "center",
                    textStyle: { color: "#9ca3af", fontSize: 14 },
                },
            });
            return;
        }

        // Collect unique time bins and directions
        var binSet = {};
        var dirSet = {};
        data.forEach(function (d) {
            binSet[d.bin_start] = true;
            dirSet[d.direction] = true;
        });

        var bins = Object.keys(binSet).sort(function (a, b) {
            return new Date(a) - new Date(b);
        });

        var directions = Object.keys(dirSet).sort();

        // Build lookup: direction -> bin -> volume
        var lookup = {};
        data.forEach(function (d) {
            if (!lookup[d.direction]) lookup[d.direction] = {};
            lookup[d.direction][d.bin_start] = (lookup[d.direction][d.bin_start] || 0) + d.volume;
        });

        var colorIdx = 0;
        var defaultColors = ["#3b82f6", "#ef4444", "#22c55e", "#f59e0b", "#8b5cf6", "#ec4899", "#06b6d4", "#f97316"];

        var series = directions.map(function (dir) {
            var color = DIRECTION_COLORS[dir] || defaultColors[colorIdx++ % defaultColors.length];
            return {
                name: dir,
                type: "bar",
                stack: "tmc",
                data: bins.map(function (bin) {
                    return (lookup[dir] && lookup[dir][bin]) || 0;
                }),
                itemStyle: { color: color },
                emphasis: { focus: "series" },
            };
        });

        var option = {
            title: {
                text: "Turning Movement Counts",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: function (params) {
                    if (!params || params.length === 0) return "";
                    var dt = new Date(params[0].axisValue);
                    var header = tsigma.dates
                        ? tsigma.dates.formatDisplay(dt.toISOString())
                        : dt.toLocaleString();
                    var total = 0;
                    var lines = params.map(function (p) {
                        total += p.value;
                        return p.marker + " " + p.seriesName + ": " + p.value;
                    });
                    lines.push("<strong>Total: " + total + "</strong>");
                    return header + "<br/>" + lines.join("<br/>");
                },
            },
            legend: {
                data: directions,
                bottom: 0,
            },
            grid: { left: 60, right: 30, top: 50, bottom: 70 },
            xAxis: {
                type: "category",
                data: bins,
                axisLabel: {
                    rotate: 45,
                    fontSize: 10,
                    formatter: function (v) {
                        var d = new Date(v);
                        return (d.getHours() < 10 ? "0" : "") + d.getHours() + ":" +
                               (d.getMinutes() < 10 ? "0" : "") + d.getMinutes();
                    },
                },
            },
            yAxis: {
                type: "value",
                name: "Volume",
                nameLocation: "center",
                nameGap: 40,
            },
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: series,
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.turningMovementCounts = turningMovementCounts;
})();
