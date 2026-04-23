/**
 * TSIGMA - Bike Volume Chart
 *
 * Multi-series line chart of bicycle volumes per channel over time bins.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var bikeVolume = {};
    var _chart = null;

    var BIKE_COLORS = [
        "#22c55e", "#06b6d4", "#3b82f6", "#10b981",
        "#0ea5e9", "#14b8a6", "#059669", "#0284c7",
    ];

    bikeVolume.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Bike volume container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    bikeVolume.render = function (data) {
        if (!_chart) throw new Error("Bike volume chart not initialized");
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

        // Group by channel
        var channelMap = {};
        data.forEach(function (d) {
            var ch = d.channel || "Unknown";
            if (!channelMap[ch]) channelMap[ch] = [];
            channelMap[ch].push(d);
        });

        var channels = Object.keys(channelMap).sort();

        var series = channels.map(function (ch, idx) {
            var points = channelMap[ch].sort(function (a, b) {
                return new Date(a.bin_start) - new Date(b.bin_start);
            });
            return {
                name: ch,
                type: "line",
                data: points.map(function (d) {
                    return [d.bin_start, d.volume];
                }),
                itemStyle: { color: BIKE_COLORS[idx % BIKE_COLORS.length] },
                lineStyle: { width: 2 },
                symbol: "circle",
                symbolSize: 4,
                emphasis: { focus: "series" },
            };
        });

        var option = {
            title: {
                text: "Bicycle Volume",
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
                        return p.marker + " " + p.seriesName + ": " + p.value[1] + " bikes";
                    });
                    return header + "<br/>" + lines.join("<br/>");
                },
            },
            legend: {
                data: channels,
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
                name: "Volume",
                nameLocation: "center",
                nameGap: 40,
                minInterval: 1,
            },
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: series,
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.bikeVolume = bikeVolume;
})();
