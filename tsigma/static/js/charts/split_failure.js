/**
 * TSIGMA - Split Failure Chart
 *
 * Dual scatter plot of green-start and red-start occupancy per cycle,
 * with split failure cycles highlighted and threshold reference lines.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var splitFailure = {};
    var _chart = null;
    var DEFAULT_THRESHOLD = 0.79;

    splitFailure.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Split failure container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    splitFailure.render = function (data, options) {
        if (!_chart) throw new Error("Split failure chart not initialized");
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

        var threshold = (options && options.threshold) || DEFAULT_THRESHOLD;

        var sorted = data.slice().sort(function (a, b) {
            return new Date(a.cycle_start) - new Date(b.cycle_start);
        });

        var totalCycles = sorted.length;
        var failureCount = sorted.filter(function (d) { return d.is_split_failure; }).length;
        var failurePct = totalCycles > 0 ? ((failureCount / totalCycles) * 100).toFixed(1) : "0.0";

        // Separate normal vs failure points for different markers
        var greenNormal = [];
        var greenFailure = [];
        var redNormal = [];
        var redFailure = [];

        sorted.forEach(function (d) {
            var point = [d.cycle_start, d.green_start_occupancy];
            var rPoint = [d.cycle_start, d.red_start_occupancy];
            if (d.is_split_failure) {
                greenFailure.push(point);
                redFailure.push(rPoint);
            } else {
                greenNormal.push(point);
                redNormal.push(rPoint);
            }
        });

        var option = {
            title: [
                {
                    text: "Split Failure Analysis",
                    left: "center",
                    textStyle: { fontSize: 14 },
                },
                {
                    text: "Total Cycles: " + totalCycles +
                          "  |  Failures: " + failureCount +
                          " (" + failurePct + "%)",
                    left: "center",
                    top: 25,
                    textStyle: { fontSize: 11, color: "#6b7280", fontWeight: "normal" },
                },
            ],
            tooltip: {
                trigger: "item",
                formatter: function (params) {
                    var dt = new Date(params.value[0]);
                    var time = tsigma.dates
                        ? tsigma.dates.formatDisplay(dt.toISOString())
                        : dt.toLocaleString();
                    return params.seriesName + "<br/>" +
                           time + "<br/>" +
                           "Occupancy: " + (params.value[1] * 100).toFixed(1) + "%";
                },
            },
            legend: {
                data: [
                    "Green Start Occ.",
                    "Red Start Occ.",
                    "Green (Failure)",
                    "Red (Failure)",
                ],
                bottom: 0,
            },
            grid: { left: 60, right: 30, top: 55, bottom: 70 },
            xAxis: {
                type: "time",
                name: "Time",
                nameLocation: "center",
                nameGap: 30,
            },
            yAxis: {
                type: "value",
                name: "Occupancy",
                nameLocation: "center",
                nameGap: 40,
                min: 0,
                max: 1,
                axisLabel: {
                    formatter: function (v) { return (v * 100).toFixed(0) + "%"; },
                },
            },
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: [
                {
                    name: "Green Start Occ.",
                    type: "scatter",
                    data: greenNormal,
                    symbolSize: 6,
                    itemStyle: { color: "#4ade80" },
                },
                {
                    name: "Red Start Occ.",
                    type: "scatter",
                    data: redNormal,
                    symbolSize: 6,
                    itemStyle: { color: "#ef4444" },
                },
                {
                    name: "Green (Failure)",
                    type: "scatter",
                    data: greenFailure,
                    symbol: "rect",
                    symbolSize: 10,
                    itemStyle: { color: "#166534", borderColor: "#000", borderWidth: 1 },
                },
                {
                    name: "Red (Failure)",
                    type: "scatter",
                    data: redFailure,
                    symbol: "rect",
                    symbolSize: 10,
                    itemStyle: { color: "#991b1b", borderColor: "#000", borderWidth: 1 },
                },
                {
                    name: "Threshold",
                    type: "line",
                    data: [],
                    markLine: {
                        silent: true,
                        symbol: "none",
                        lineStyle: { type: "dashed", color: "#6366f1", width: 1.5 },
                        data: [
                            {
                                yAxis: threshold,
                                label: {
                                    formatter: "Threshold: " + (threshold * 100).toFixed(0) + "%",
                                    position: "insideEndTop",
                                },
                            },
                        ],
                    },
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.splitFailure = splitFailure;
})();
