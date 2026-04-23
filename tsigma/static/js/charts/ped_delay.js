/**
 * TSIGMA - Pedestrian Delay Chart
 *
 * Bar chart of average ped delay per phase with min/max whiskers
 * and press-count annotations.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var pedDelay = {};
    var _chart = null;

    /**
     * Initialize an ECharts instance.
     * @param {string} containerId
     * @returns {echarts.ECharts}
     */
    pedDelay.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Ped delay container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    /**
     * Render pedestrian delay data.
     * @param {Array<Object>} data  List of ped delay objects per phase.
     */
    pedDelay.render = function (data) {
        if (!_chart) throw new Error("Ped delay chart not initialized");
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

        var phases = data.map(function (d) { return "Ped " + d.phase; });
        var avgDelays = data.map(function (d) { return d.avg_delay; });
        var minDelays = data.map(function (d) { return d.min_delay; });
        var maxDelays = data.map(function (d) { return d.max_delay; });

        // Build candlestick-style error bars using custom series
        var whiskerData = data.map(function (d, i) {
            return [i, d.min_delay, d.avg_delay, d.avg_delay, d.max_delay];
        });

        var option = {
            title: {
                text: "Pedestrian Delay",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: function (params) {
                    var idx = params[0].dataIndex;
                    var d = data[idx];
                    return (
                        phases[idx] +
                        "<br/>Avg Delay: " + d.avg_delay.toFixed(1) + "s" +
                        "<br/>Min Delay: " + d.min_delay.toFixed(1) + "s" +
                        "<br/>Max Delay: " + d.max_delay.toFixed(1) + "s" +
                        "<br/>Presses: " + d.presses
                    );
                },
            },
            grid: { left: 60, right: 40, top: 50, bottom: 40 },
            xAxis: {
                type: "category",
                data: phases,
            },
            yAxis: {
                type: "value",
                name: "Delay (s)",
                nameLocation: "center",
                nameGap: 40,
                min: 0,
            },
            series: [
                {
                    name: "Avg Delay",
                    type: "bar",
                    data: avgDelays,
                    itemStyle: { color: "#6366f1" },
                    barMaxWidth: 40,
                    label: {
                        show: true,
                        position: "top",
                        formatter: function (params) {
                            return data[params.dataIndex].presses + " presses";
                        },
                        fontSize: 10,
                        color: "#4b5563",
                    },
                },
                {
                    name: "Min/Max Range",
                    type: "custom",
                    renderItem: function (params, api) {
                        var categoryIdx = api.value(0);
                        var minVal = api.value(1);
                        var maxVal = api.value(4);
                        var coordMin = api.coord([categoryIdx, minVal]);
                        var coordMax = api.coord([categoryIdx, maxVal]);
                        var barWidth = 12;

                        return {
                            type: "group",
                            children: [
                                // Vertical line (whisker)
                                {
                                    type: "line",
                                    shape: {
                                        x1: coordMin[0],
                                        y1: coordMin[1],
                                        x2: coordMax[0],
                                        y2: coordMax[1],
                                    },
                                    style: {
                                        stroke: "#4338ca",
                                        lineWidth: 2,
                                    },
                                },
                                // Bottom cap
                                {
                                    type: "line",
                                    shape: {
                                        x1: coordMin[0] - barWidth / 2,
                                        y1: coordMin[1],
                                        x2: coordMin[0] + barWidth / 2,
                                        y2: coordMin[1],
                                    },
                                    style: {
                                        stroke: "#4338ca",
                                        lineWidth: 2,
                                    },
                                },
                                // Top cap
                                {
                                    type: "line",
                                    shape: {
                                        x1: coordMax[0] - barWidth / 2,
                                        y1: coordMax[1],
                                        x2: coordMax[0] + barWidth / 2,
                                        y2: coordMax[1],
                                    },
                                    style: {
                                        stroke: "#4338ca",
                                        lineWidth: 2,
                                    },
                                },
                            ],
                        };
                    },
                    data: whiskerData,
                    z: 10,
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.pedDelay = pedDelay;
})();
