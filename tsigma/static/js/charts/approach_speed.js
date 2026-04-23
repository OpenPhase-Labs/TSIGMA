/**
 * TSIGMA - Approach Speed Chart
 *
 * Horizontal bar chart showing p15, average, and p85 speeds per approach
 * direction with a speed limit reference line.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var approachSpeed = {};
    var _chart = null;

    approachSpeed.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Approach speed container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    approachSpeed.render = function (data) {
        if (!_chart) throw new Error("Approach speed chart not initialized");
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

        var approaches = data.map(function (d) {
            return d.direction || ("Approach " + d.approach_id);
        });

        var speedLimit = null;
        for (var i = 0; i < data.length; i++) {
            if (data[i].speed_limit != null) {
                speedLimit = data[i].speed_limit;
                break;
            }
        }

        var markLineConfig = speedLimit != null ? {
            silent: true,
            symbol: "none",
            lineStyle: { type: "dashed", color: "#ef4444", width: 2 },
            data: [
                {
                    xAxis: speedLimit,
                    label: {
                        formatter: "Speed Limit: " + speedLimit + " mph",
                        position: "insideEndTop",
                    },
                },
            ],
        } : undefined;

        var option = {
            title: {
                text: "Approach Speed",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: function (params) {
                    if (!params || params.length === 0) return "";
                    var header = params[0].axisValueLabel;
                    var idx = params[0].dataIndex;
                    var d = data[idx];
                    var lines = params.map(function (p) {
                        return p.marker + " " + p.seriesName + ": " + p.value + " mph";
                    });
                    lines.push("Sample Count: " + (d.sample_count || 0));
                    return header + "<br/>" + lines.join("<br/>");
                },
            },
            legend: {
                data: ["15th Percentile", "Average", "85th Percentile"],
                bottom: 0,
            },
            grid: { left: 100, right: 40, top: 50, bottom: 50 },
            yAxis: {
                type: "category",
                data: approaches,
                axisLabel: { fontSize: 12 },
            },
            xAxis: {
                type: "value",
                name: "Speed (mph)",
                nameLocation: "center",
                nameGap: 30,
            },
            series: [
                {
                    name: "15th Percentile",
                    type: "bar",
                    data: data.map(function (d) { return d.p15_speed || 0; }),
                    itemStyle: { color: "#93c5fd" },
                    barGap: "10%",
                    label: {
                        show: true,
                        position: "right",
                        fontSize: 10,
                        formatter: function (p) { return p.value.toFixed(1); },
                    },
                    markLine: markLineConfig,
                },
                {
                    name: "Average",
                    type: "bar",
                    data: data.map(function (d) { return d.avg_speed || 0; }),
                    itemStyle: { color: "#3b82f6" },
                    label: {
                        show: true,
                        position: "right",
                        fontSize: 10,
                        formatter: function (p) { return p.value.toFixed(1); },
                    },
                },
                {
                    name: "85th Percentile",
                    type: "bar",
                    data: data.map(function (d) { return d.p85_speed || 0; }),
                    itemStyle: { color: "#1d4ed8" },
                    label: {
                        show: true,
                        position: "right",
                        fontSize: 10,
                        formatter: function (p) {
                            var idx = p.dataIndex;
                            return p.value.toFixed(1) + " (n=" + (data[idx].sample_count || 0) + ")";
                        },
                    },
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.approachSpeed = approachSpeed;
})();
