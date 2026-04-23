/**
 * TSIGMA - Preemption Chart
 *
 * Side-by-side bar charts: preemption count by type (left) and
 * average duration with min/max annotations (right).
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var preemption = {};
    var _chart = null;

    /**
     * Initialize an ECharts instance.
     * @param {string} containerId
     * @returns {echarts.ECharts}
     */
    preemption.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Preemption container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    /**
     * Render preemption data.
     * @param {Array<Object>} data  List of preemption type objects.
     */
    preemption.render = function (data) {
        if (!_chart) throw new Error("Preemption chart not initialized");
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

        var types = data.map(function (d) { return d.type; });
        var counts = data.map(function (d) { return d.count; });
        var avgDurations = data.map(function (d) { return d.avg_duration; });
        var minDurations = data.map(function (d) { return d.min_duration; });
        var maxDurations = data.map(function (d) { return d.max_duration; });

        var option = {
            title: [
                {
                    text: "Preemption Count",
                    left: "22%",
                    top: 0,
                    textAlign: "center",
                    textStyle: { fontSize: 14 },
                },
                {
                    text: "Avg Duration",
                    left: "72%",
                    top: 0,
                    textAlign: "center",
                    textStyle: { fontSize: 14 },
                },
            ],
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
            },
            grid: [
                { left: 60, right: "55%", top: 40, bottom: 40 },
                { left: "55%", right: 40, top: 40, bottom: 40 },
            ],
            xAxis: [
                {
                    type: "category",
                    data: types,
                    gridIndex: 0,
                    axisLabel: { rotate: 30, fontSize: 11 },
                },
                {
                    type: "category",
                    data: types,
                    gridIndex: 1,
                    axisLabel: { rotate: 30, fontSize: 11 },
                },
            ],
            yAxis: [
                {
                    type: "value",
                    name: "Count",
                    nameLocation: "center",
                    nameGap: 35,
                    gridIndex: 0,
                },
                {
                    type: "value",
                    name: "Duration (s)",
                    nameLocation: "center",
                    nameGap: 35,
                    gridIndex: 1,
                },
            ],
            series: [
                {
                    name: "Count",
                    type: "bar",
                    data: counts,
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    itemStyle: { color: "#f97316" },
                    barMaxWidth: 40,
                    label: {
                        show: true,
                        position: "top",
                        fontSize: 11,
                    },
                },
                {
                    name: "Avg Duration",
                    type: "bar",
                    data: avgDurations,
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    itemStyle: { color: "#fbbf24" },
                    barMaxWidth: 40,
                    label: {
                        show: true,
                        position: "top",
                        formatter: function (params) {
                            return params.value.toFixed(1) + "s";
                        },
                        fontSize: 10,
                    },
                },
                {
                    name: "Duration Range",
                    type: "custom",
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    renderItem: function (params, api) {
                        var idx = params.dataIndex;
                        var minVal = minDurations[idx];
                        var maxVal = maxDurations[idx];
                        var catCoord = api.coord([idx, minVal]);
                        var topCoord = api.coord([idx, maxVal]);
                        var barWidth = 12;

                        return {
                            type: "group",
                            children: [
                                {
                                    type: "line",
                                    shape: {
                                        x1: catCoord[0],
                                        y1: catCoord[1],
                                        x2: topCoord[0],
                                        y2: topCoord[1],
                                    },
                                    style: { stroke: "#d97706", lineWidth: 2 },
                                },
                                {
                                    type: "line",
                                    shape: {
                                        x1: catCoord[0] - barWidth / 2,
                                        y1: catCoord[1],
                                        x2: catCoord[0] + barWidth / 2,
                                        y2: catCoord[1],
                                    },
                                    style: { stroke: "#d97706", lineWidth: 2 },
                                },
                                {
                                    type: "line",
                                    shape: {
                                        x1: topCoord[0] - barWidth / 2,
                                        y1: topCoord[1],
                                        x2: topCoord[0] + barWidth / 2,
                                        y2: topCoord[1],
                                    },
                                    style: { stroke: "#d97706", lineWidth: 2 },
                                },
                            ],
                        };
                    },
                    data: data.map(function (d, i) {
                        return [i, d.min_duration, d.max_duration];
                    }),
                    z: 10,
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.preemption = preemption;
})();
