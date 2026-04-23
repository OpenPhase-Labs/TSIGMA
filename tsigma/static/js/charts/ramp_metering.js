/**
 * TSIGMA - Ramp Metering Chart
 *
 * Dual-axis chart: stacked bars for demand/passage volume on the left axis,
 * lines for metering rate and queue occupancy percentage on the right axis.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var rampMetering = {};
    var _chart = null;

    rampMetering.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Ramp metering container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    rampMetering.render = function (data) {
        if (!_chart) throw new Error("Ramp metering chart not initialized");
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
        var demandVolume = sorted.map(function (d) { return d.demand_volume || 0; });
        var passageVolume = sorted.map(function (d) { return d.passage_volume || 0; });
        var meteringRate = sorted.map(function (d) { return d.metering_rate || 0; });
        var queueOccupancy = sorted.map(function (d) { return d.queue_occupancy_pct || 0; });

        var option = {
            title: {
                text: "Ramp Metering",
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
                    var idx = params[0].dataIndex;
                    var d = sorted[idx];
                    var lines = [
                        header,
                        "Demand Volume: " + (d.demand_volume || 0),
                        "Passage Volume: " + (d.passage_volume || 0),
                        "Metering Rate: " + (d.metering_rate || 0) + " veh/hr",
                        "Avg Green: " + ((d.avg_green_seconds || 0).toFixed(1)) + "s",
                        "Queue Occupancy: " + ((d.queue_occupancy_pct || 0).toFixed(1)) + "%",
                    ];
                    return lines.join("<br/>");
                },
            },
            legend: {
                data: ["Demand Volume", "Passage Volume", "Metering Rate", "Queue Occupancy"],
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
                    name: "Volume",
                    nameLocation: "center",
                    nameGap: 45,
                    position: "left",
                },
                {
                    type: "value",
                    name: "Rate / Occupancy",
                    nameLocation: "center",
                    nameGap: 50,
                    position: "right",
                },
            ],
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: [
                {
                    name: "Demand Volume",
                    type: "bar",
                    stack: "volume",
                    data: demandVolume,
                    itemStyle: { color: "#3b82f6" },
                    yAxisIndex: 0,
                },
                {
                    name: "Passage Volume",
                    type: "bar",
                    stack: "volume",
                    data: passageVolume,
                    itemStyle: { color: "#22c55e" },
                    yAxisIndex: 0,
                },
                {
                    name: "Metering Rate",
                    type: "line",
                    data: meteringRate,
                    itemStyle: { color: "#f59e0b" },
                    lineStyle: { width: 2 },
                    symbol: "circle",
                    symbolSize: 4,
                    yAxisIndex: 1,
                },
                {
                    name: "Queue Occupancy",
                    type: "line",
                    data: queueOccupancy,
                    itemStyle: { color: "#ef4444" },
                    lineStyle: { width: 2 },
                    symbol: "circle",
                    symbolSize: 4,
                    yAxisIndex: 1,
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.rampMetering = rampMetering;
})();
