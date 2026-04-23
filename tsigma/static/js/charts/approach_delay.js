/**
 * TSIGMA - Approach Delay Chart
 *
 * Horizontal bar chart showing average and max delay per approach.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var approachDelay = {};
    var _chart = null;

    /**
     * Initialize an ECharts instance.
     * @param {string} containerId
     * @returns {echarts.ECharts}
     */
    approachDelay.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Approach delay container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    /**
     * Render approach delay data.
     * @param {Array<Object>} data  List of approach delay objects.
     */
    approachDelay.render = function (data) {
        if (!_chart) throw new Error("Approach delay chart not initialized");
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

        var labels = data.map(function (d) {
            return d.approach + " " + d.direction;
        });
        var avgDelays = data.map(function (d) { return d.avg_delay; });
        var maxDelays = data.map(function (d) { return d.max_delay; });
        var samples = data.map(function (d) { return d.sample_count; });

        var option = {
            title: {
                text: "Approach Delay",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: function (params) {
                    var idx = params[0].dataIndex;
                    var lines = [
                        params[0].axisValueLabel,
                        "Avg Delay: " + avgDelays[idx].toFixed(1) + "s",
                        "Max Delay: " + maxDelays[idx].toFixed(1) + "s",
                        "Samples: " + samples[idx],
                    ];
                    return lines.join("<br/>");
                },
            },
            legend: { data: ["Avg Delay", "Max Delay"], bottom: 0 },
            grid: { left: 120, right: 80, top: 40, bottom: 40 },
            yAxis: {
                type: "category",
                data: labels,
                axisLabel: { fontSize: 12 },
            },
            xAxis: {
                type: "value",
                name: "Delay (s)",
                nameLocation: "center",
                nameGap: 30,
            },
            series: [
                {
                    name: "Max Delay",
                    type: "bar",
                    data: maxDelays,
                    itemStyle: { color: "rgba(96, 165, 250, 0.35)" },
                    barGap: "-100%",
                    z: 1,
                },
                {
                    name: "Avg Delay",
                    type: "bar",
                    data: avgDelays,
                    itemStyle: { color: "#3b82f6" },
                    z: 2,
                    label: {
                        show: true,
                        position: "right",
                        formatter: function (params) {
                            return "n=" + samples[params.dataIndex];
                        },
                        fontSize: 10,
                        color: "#6b7280",
                    },
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.approachDelay = approachDelay;
})();
