/**
 * TSIGMA - Approach Volume Chart
 *
 * Multi-series line chart of vehicle volumes per approach direction
 * over time bins.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var approachVolume = {};
    var _chart = null;

    var DIRECTION_COLORS = {
        NB: "#3b82f6",
        SB: "#ef4444",
        EB: "#22c55e",
        WB: "#f59e0b",
        NEB: "#8b5cf6",
        SEB: "#ec4899",
        NWB: "#06b6d4",
        SWB: "#f97316",
    };

    approachVolume.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Approach volume container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    approachVolume.render = function (data) {
        if (!_chart) throw new Error("Approach volume chart not initialized");
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

        // Group by direction
        var dirMap = {};
        data.forEach(function (d) {
            var dir = d.direction || ("Approach " + d.approach_id);
            if (!dirMap[dir]) dirMap[dir] = [];
            dirMap[dir].push(d);
        });

        var directions = Object.keys(dirMap);
        var colorIdx = 0;
        var defaultColors = ["#3b82f6", "#ef4444", "#22c55e", "#f59e0b", "#8b5cf6", "#ec4899", "#06b6d4", "#f97316"];

        var series = directions.map(function (dir) {
            var points = dirMap[dir].sort(function (a, b) {
                return new Date(a.bin_start) - new Date(b.bin_start);
            });
            var color = DIRECTION_COLORS[dir] || defaultColors[colorIdx++ % defaultColors.length];
            return {
                name: dir,
                type: "line",
                data: points.map(function (d) {
                    return [d.bin_start, d.volume];
                }),
                itemStyle: { color: color },
                lineStyle: { width: 2 },
                symbol: "circle",
                symbolSize: 4,
                emphasis: { focus: "series" },
            };
        });

        var option = {
            title: {
                text: "Approach Volume",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "axis",
                formatter: function (params) {
                    if (!params || params.length === 0) return "";
                    var dt = new Date(params[0].value[0]);
                    var header = tsigma.dates
                        ? tsigma.dates.formatDisplay(dt.toISOString())
                        : dt.toLocaleString();
                    var lines = params.map(function (p) {
                        return p.marker + " " + p.seriesName + ": " + p.value[1] + " vehicles";
                    });
                    return header + "<br/>" + lines.join("<br/>");
                },
            },
            legend: {
                data: directions,
                bottom: 0,
            },
            grid: { left: 60, right: 30, top: 50, bottom: 70 },
            xAxis: {
                type: "time",
                name: "Time",
                nameLocation: "center",
                nameGap: 30,
            },
            yAxis: {
                type: "value",
                name: "Volume (vehicles)",
                nameLocation: "center",
                nameGap: 45,
            },
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: series,
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.approachVolume = approachVolume;
})();
