/**
 * TSIGMA - Left Turn Gap Analysis Chart
 *
 * Stacked bar chart of sufficient/marginal/insufficient gaps per cycle
 * with an overlay line for average gap duration on a secondary Y axis.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var leftTurnGap = {};
    var _chart = null;

    leftTurnGap.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Left turn gap container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    leftTurnGap.render = function (data) {
        if (!_chart) throw new Error("Left turn gap chart not initialized");
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

        var option = {
            title: {
                text: "Left Turn Gap Analysis",
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
                    var lines = params.map(function (p) {
                        var unit = p.seriesName === "Avg Gap Duration" ? "s" : "";
                        return p.marker + " " + p.seriesName + ": " + p.value[1] + unit;
                    });
                    return header + "<br/>" + lines.join("<br/>");
                },
            },
            legend: {
                data: ["Sufficient", "Marginal", "Insufficient", "Avg Gap Duration"],
                bottom: 0,
            },
            grid: { left: 60, right: 60, top: 50, bottom: 70 },
            xAxis: {
                type: "time",
                name: "Cycle",
                nameLocation: "center",
                nameGap: 30,
            },
            yAxis: [
                {
                    type: "value",
                    name: "Gap Count",
                    nameLocation: "center",
                    nameGap: 40,
                },
                {
                    type: "value",
                    name: "Avg Duration (s)",
                    nameLocation: "center",
                    nameGap: 40,
                },
            ],
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: [
                {
                    name: "Sufficient",
                    type: "bar",
                    stack: "gaps",
                    data: sorted.map(function (d) {
                        return [d.cycle_start, d.sufficient_gaps || 0];
                    }),
                    itemStyle: { color: "#4ade80" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Marginal",
                    type: "bar",
                    stack: "gaps",
                    data: sorted.map(function (d) {
                        return [d.cycle_start, d.marginal_gaps || 0];
                    }),
                    itemStyle: { color: "#fbbf24" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Insufficient",
                    type: "bar",
                    stack: "gaps",
                    data: sorted.map(function (d) {
                        return [d.cycle_start, d.insufficient_gaps || 0];
                    }),
                    itemStyle: { color: "#ef4444" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Avg Gap Duration",
                    type: "line",
                    yAxisIndex: 1,
                    data: sorted.map(function (d) {
                        return [d.cycle_start, d.avg_gap_duration || 0];
                    }),
                    itemStyle: { color: "#6366f1" },
                    lineStyle: { width: 2 },
                    symbol: "circle",
                    symbolSize: 4,
                    z: 10,
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.leftTurnGap = leftTurnGap;
})();
