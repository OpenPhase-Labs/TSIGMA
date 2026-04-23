/**
 * TSIGMA - Link Pivot Chart
 *
 * Horizontal bar chart showing corridor signal-to-signal offsets
 * with standard deviation error bars.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var linkPivot = {};
    var _chart = null;

    linkPivot.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Link pivot container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    linkPivot.render = function (data) {
        if (!_chart) throw new Error("Link pivot chart not initialized");
        if (!data || !data.offsets || data.offsets.length === 0) {
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

        var offsets = data.offsets;
        var routeName = data.route_name || "Corridor";

        var labels = offsets.map(function (o) {
            return o.from_signal + " \u2192 " + o.to_signal;
        });

        var avgValues = offsets.map(function (o) { return o.avg_offset || 0; });

        // Error bar data: [index, low, high]
        var errorData = offsets.map(function (o, i) {
            var avg = o.avg_offset || 0;
            var std = o.stddev_offset || 0;
            return [i, avg - std, avg + std, avg];
        });

        var option = {
            title: {
                text: "Link Pivot: " + routeName,
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: function (params) {
                    var idx = params[0].dataIndex;
                    var o = offsets[idx];
                    var lines = [
                        labels[idx],
                        "Avg Offset: " + (o.avg_offset || 0).toFixed(1) + "s",
                        "Std Dev: \u00b1" + (o.stddev_offset || 0).toFixed(1) + "s",
                        "Samples: " + (o.sample_count || 0),
                    ];
                    return lines.join("<br/>");
                },
            },
            grid: { left: 160, right: 40, top: 50, bottom: 40 },
            yAxis: {
                type: "category",
                data: labels,
                axisLabel: { fontSize: 11 },
                inverse: true,
            },
            xAxis: {
                type: "value",
                name: "Offset (seconds)",
                nameLocation: "center",
                nameGap: 30,
            },
            series: [
                {
                    name: "Avg Offset",
                    type: "bar",
                    data: avgValues,
                    itemStyle: { color: "#3b82f6" },
                    label: {
                        show: true,
                        position: "right",
                        fontSize: 10,
                        formatter: function (p) { return p.value.toFixed(1) + "s"; },
                    },
                },
                {
                    name: "Std Dev",
                    type: "custom",
                    renderItem: function (params, api) {
                        var categoryIndex = api.value(0);
                        var low = api.value(1);
                        var high = api.value(2);

                        var lowPt = api.coord([low, categoryIndex]);
                        var highPt = api.coord([high, categoryIndex]);
                        var halfHeight = api.size([0, 1])[1] * 0.15;

                        var style = api.style({
                            stroke: "#1e3a5f",
                            fill: null,
                            lineWidth: 1.5,
                        });

                        return {
                            type: "group",
                            children: [
                                {
                                    type: "line",
                                    shape: {
                                        x1: lowPt[0],
                                        y1: lowPt[1],
                                        x2: highPt[0],
                                        y2: highPt[1],
                                    },
                                    style: style,
                                },
                                {
                                    type: "line",
                                    shape: {
                                        x1: lowPt[0],
                                        y1: lowPt[1] - halfHeight,
                                        x2: lowPt[0],
                                        y2: lowPt[1] + halfHeight,
                                    },
                                    style: style,
                                },
                                {
                                    type: "line",
                                    shape: {
                                        x1: highPt[0],
                                        y1: highPt[1] - halfHeight,
                                        x2: highPt[0],
                                        y2: highPt[1] + halfHeight,
                                    },
                                    style: style,
                                },
                            ],
                        };
                    },
                    data: errorData,
                    z: 10,
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.linkPivot = linkPivot;
})();
