/**
 * TSIGMA - Transit Signal Priority Chart
 *
 * Dual-axis chart: stacked bars for TSP requests/adjustments/checkouts
 * on the left axis, and lines for average green time with/without TSP
 * on the right axis.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var transitSignalPriority = {};
    var _chart = null;

    transitSignalPriority.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Transit signal priority container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    transitSignalPriority.render = function (data) {
        if (!_chart) throw new Error("Transit signal priority chart not initialized");
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
            return new Date(a.time_bin) - new Date(b.time_bin);
        });

        var timeBins = sorted.map(function (d) { return d.time_bin; });
        var requests = sorted.map(function (d) { return d.tsp_requests || 0; });
        var adjustments = sorted.map(function (d) { return d.tsp_adjustments || 0; });
        var checkouts = sorted.map(function (d) { return d.tsp_checkouts || 0; });
        var greenWithTsp = sorted.map(function (d) { return d.avg_green_with_tsp; });
        var greenWithoutTsp = sorted.map(function (d) { return d.avg_green_without_tsp; });

        var hasGreenWithTsp = greenWithTsp.some(function (v) { return v != null; });
        var hasGreenWithoutTsp = greenWithoutTsp.some(function (v) { return v != null; });

        var legendData = ["TSP Requests", "TSP Adjustments", "TSP Checkouts"];
        var seriesList = [
            {
                name: "TSP Requests",
                type: "bar",
                stack: "tsp",
                data: requests,
                itemStyle: { color: "#3b82f6" },
                yAxisIndex: 0,
            },
            {
                name: "TSP Adjustments",
                type: "bar",
                stack: "tsp",
                data: adjustments,
                itemStyle: { color: "#22c55e" },
                yAxisIndex: 0,
            },
            {
                name: "TSP Checkouts",
                type: "bar",
                stack: "tsp",
                data: checkouts,
                itemStyle: { color: "#f59e0b" },
                yAxisIndex: 0,
            },
        ];

        if (hasGreenWithTsp) {
            legendData.push("Avg Green (with TSP)");
            seriesList.push({
                name: "Avg Green (with TSP)",
                type: "line",
                data: greenWithTsp,
                itemStyle: { color: "#8b5cf6" },
                lineStyle: { width: 2 },
                symbol: "circle",
                symbolSize: 4,
                yAxisIndex: 1,
                connectNulls: true,
            });
        }

        if (hasGreenWithoutTsp) {
            legendData.push("Avg Green (without TSP)");
            seriesList.push({
                name: "Avg Green (without TSP)",
                type: "line",
                data: greenWithoutTsp,
                itemStyle: { color: "#ef4444" },
                lineStyle: { width: 2, type: "dashed" },
                symbol: "diamond",
                symbolSize: 4,
                yAxisIndex: 1,
                connectNulls: true,
            });
        }

        var option = {
            title: {
                text: "Transit Signal Priority",
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
                    var lines = params.map(function (p) {
                        var val = p.value;
                        var suffix = "";
                        if (p.seriesName.indexOf("Avg Green") === 0) {
                            suffix = val != null ? val.toFixed(1) + "s" : "N/A";
                        } else {
                            suffix = val != null ? val.toString() : "0";
                        }
                        return p.marker + " " + p.seriesName + ": " + suffix;
                    });
                    return header + "<br/>" + lines.join("<br/>");
                },
            },
            legend: {
                data: legendData,
                top: 30,
            },
            grid: { left: 60, right: 70, top: 70, bottom: 70 },
            xAxis: {
                type: "category",
                data: timeBins,
                axisLabel: {
                    formatter: function (v) {
                        var dt = new Date(v);
                        return dt.getHours() + ":" + (dt.getMinutes() < 10 ? "0" : "") + dt.getMinutes();
                    },
                    rotate: 45,
                },
            },
            yAxis: [
                {
                    type: "value",
                    name: "Count",
                    nameLocation: "center",
                    nameGap: 45,
                    position: "left",
                },
                {
                    type: "value",
                    name: "Avg Green (s)",
                    nameLocation: "center",
                    nameGap: 50,
                    position: "right",
                },
            ],
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: seriesList,
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.transitSignalPriority = transitSignalPriority;
})();
