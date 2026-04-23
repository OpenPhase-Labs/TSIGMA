/**
 * TSIGMA - Yellow/Red Actuations Chart
 *
 * Stacked bar chart of green, yellow, and red actuations per cycle
 * with a reference line for average total actuations.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var yellowRedActuations = {};
    var _chart = null;

    yellowRedActuations.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Yellow/Red actuations container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    yellowRedActuations.render = function (data) {
        if (!_chart) throw new Error("Yellow/Red actuations chart not initialized");
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

        var sorted = data.slice().sort(function (a, b) {
            return new Date(a.cycle_start) - new Date(b.cycle_start);
        });

        var totalSum = sorted.reduce(function (s, d) {
            return s + (d.total_actuations || 0);
        }, 0);
        var avgTotal = sorted.length > 0 ? totalSum / sorted.length : 0;

        var option = {
            title: {
                text: "Yellow / Red Actuations",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: function (params) {
                    if (!params || params.length === 0) return "";
                    var dt = new Date(params[0].value[0]);
                    var header = tsigma.dates
                        ? tsigma.dates.formatDisplay(dt.toISOString())
                        : dt.toLocaleString();
                    var lines = params
                        .filter(function (p) { return p.seriesType === "bar"; })
                        .map(function (p) {
                            return p.marker + " " + p.seriesName + ": " + p.value[1];
                        });
                    return header + "<br/>" + lines.join("<br/>");
                },
            },
            legend: {
                data: ["Green", "Yellow", "Red", "Average Total"],
                bottom: 0,
            },
            grid: { left: 60, right: 30, top: 50, bottom: 70 },
            xAxis: {
                type: "time",
                name: "Cycle Time",
                nameLocation: "center",
                nameGap: 30,
            },
            yAxis: {
                type: "value",
                name: "Count",
                nameLocation: "center",
                nameGap: 40,
            },
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: [
                {
                    name: "Green",
                    type: "bar",
                    stack: "actuations",
                    data: sorted.map(function (d) {
                        return [d.cycle_start, d.green_actuations || 0];
                    }),
                    itemStyle: { color: "#4ade80" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Yellow",
                    type: "bar",
                    stack: "actuations",
                    data: sorted.map(function (d) {
                        return [d.cycle_start, d.yellow_actuations || 0];
                    }),
                    itemStyle: { color: "#fbbf24" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Red",
                    type: "bar",
                    stack: "actuations",
                    data: sorted.map(function (d) {
                        return [d.cycle_start, d.red_actuations || 0];
                    }),
                    itemStyle: { color: "#ef4444" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Average Total",
                    type: "line",
                    markLine: {
                        silent: true,
                        symbol: "none",
                        lineStyle: { type: "dashed", color: "#6366f1", width: 2 },
                        data: [
                            {
                                yAxis: avgTotal,
                                label: {
                                    formatter: "Avg: " + avgTotal.toFixed(1),
                                    position: "insideEndTop",
                                },
                            },
                        ],
                    },
                    data: [],
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.yellowRedActuations = yellowRedActuations;
})();
