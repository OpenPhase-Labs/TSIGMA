/**
 * TSIGMA - Wait Time Chart
 *
 * Bar chart with min/max whiskers showing average wait time per cycle,
 * with arrivals during red on a secondary Y axis.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var waitTime = {};
    var _chart = null;

    waitTime.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Wait time container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    waitTime.render = function (data) {
        if (!_chart) throw new Error("Wait time chart not initialized");
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

        // Build whisker data for custom renderItem
        var whiskerData = sorted.map(function (d, i) {
            return [i, d.min_wait_time || 0, d.max_wait_time || 0, d.avg_wait_time || 0];
        });

        var categories = sorted.map(function (d) { return d.cycle_start; });

        var option = {
            title: {
                text: "Wait Time Analysis",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: function (params) {
                    if (!params || params.length === 0) return "";
                    var idx = params[0].dataIndex;
                    var d = sorted[idx];
                    var dt = new Date(d.cycle_start);
                    var header = tsigma.dates
                        ? tsigma.dates.formatDisplay(dt.toISOString())
                        : dt.toLocaleString();
                    var lines = [
                        "Avg Wait: " + (d.avg_wait_time || 0).toFixed(1) + "s",
                        "Min Wait: " + (d.min_wait_time || 0).toFixed(1) + "s",
                        "Max Wait: " + (d.max_wait_time || 0).toFixed(1) + "s",
                    ];
                    var arrParam = params.find(function (p) {
                        return p.seriesName === "Arrivals During Red";
                    });
                    if (arrParam) {
                        lines.push("Arrivals: " + arrParam.value);
                    }
                    return header + "<br/>" + lines.join("<br/>");
                },
            },
            legend: {
                data: ["Avg Wait Time", "Min/Max Range", "Arrivals During Red"],
                bottom: 0,
            },
            grid: { left: 60, right: 60, top: 50, bottom: 70 },
            xAxis: {
                type: "category",
                data: categories,
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
            yAxis: [
                {
                    type: "value",
                    name: "Wait Time (s)",
                    nameLocation: "center",
                    nameGap: 40,
                },
                {
                    type: "value",
                    name: "Arrivals",
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
                    name: "Avg Wait Time",
                    type: "bar",
                    data: sorted.map(function (d) { return d.avg_wait_time || 0; }),
                    itemStyle: { color: "#3b82f6" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Min/Max Range",
                    type: "custom",
                    renderItem: function (params, api) {
                        var categoryIndex = api.value(0);
                        var minVal = api.value(1);
                        var maxVal = api.value(2);
                        var avgVal = api.value(3);

                        var highPoint = api.coord([categoryIndex, maxVal]);
                        var lowPoint = api.coord([categoryIndex, minVal]);
                        var avgPoint = api.coord([categoryIndex, avgVal]);

                        var halfWidth = api.size([1, 0])[0] * 0.15;

                        var style = api.style({
                            stroke: "#1e3a5f",
                            fill: null,
                            lineWidth: 1.5,
                        });

                        return {
                            type: "group",
                            children: [
                                // Vertical line from min to max
                                {
                                    type: "line",
                                    shape: {
                                        x1: highPoint[0],
                                        y1: highPoint[1],
                                        x2: lowPoint[0],
                                        y2: lowPoint[1],
                                    },
                                    style: style,
                                },
                                // Top whisker
                                {
                                    type: "line",
                                    shape: {
                                        x1: highPoint[0] - halfWidth,
                                        y1: highPoint[1],
                                        x2: highPoint[0] + halfWidth,
                                        y2: highPoint[1],
                                    },
                                    style: style,
                                },
                                // Bottom whisker
                                {
                                    type: "line",
                                    shape: {
                                        x1: lowPoint[0] - halfWidth,
                                        y1: lowPoint[1],
                                        x2: lowPoint[0] + halfWidth,
                                        y2: lowPoint[1],
                                    },
                                    style: style,
                                },
                            ],
                        };
                    },
                    data: whiskerData,
                    z: 10,
                },
                {
                    name: "Arrivals During Red",
                    type: "scatter",
                    yAxisIndex: 1,
                    data: sorted.map(function (d) { return d.arrivals_during_red || 0; }),
                    symbolSize: 6,
                    itemStyle: { color: "#f97316" },
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.waitTime = waitTime;
})();
