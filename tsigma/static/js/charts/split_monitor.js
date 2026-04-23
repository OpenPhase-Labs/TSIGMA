/**
 * TSIGMA - Split Monitor Chart
 *
 * Stacked bar chart of green/yellow/red durations per phase,
 * with a companion termination-type pie chart.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var splitMonitor = {};
    var _chart = null;

    /**
     * Initialize an ECharts instance.
     * @param {string} containerId
     * @returns {echarts.ECharts}
     */
    splitMonitor.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Split monitor container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    /**
     * Render split monitor data.
     * @param {Array<Object>} data  List of phase split objects.
     */
    splitMonitor.render = function (data) {
        if (!_chart) throw new Error("Split monitor chart not initialized");
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

        var phases = data.map(function (d) {
            return "Phase " + d.phase;
        });
        var greens = data.map(function (d) { return d.avg_green; });
        var yellows = data.map(function (d) { return d.avg_yellow; });
        var reds = data.map(function (d) { return d.avg_red; });

        // Aggregate termination percentages across all phases for the pie
        var totalCycles = data.reduce(function (s, d) { return s + (d.cycles || 0); }, 0);
        var gapOutTotal = 0;
        var forceOffTotal = 0;
        var maxOutTotal = 0;

        data.forEach(function (d) {
            var c = d.cycles || 1;
            gapOutTotal += (d.gap_out_pct || 0) * c;
            forceOffTotal += (d.force_off_pct || 0) * c;
            maxOutTotal += (d.max_out_pct || 0) * c;
        });

        var pieData = [
            {
                name: "Gap Out",
                value: totalCycles ? +(gapOutTotal / totalCycles).toFixed(1) : 0,
            },
            {
                name: "Force Off",
                value: totalCycles ? +(forceOffTotal / totalCycles).toFixed(1) : 0,
            },
            {
                name: "Max Out",
                value: totalCycles ? +(maxOutTotal / totalCycles).toFixed(1) : 0,
            },
        ];

        var option = {
            title: [
                {
                    text: "Phase Split Durations",
                    left: "25%",
                    top: 0,
                    textAlign: "center",
                    textStyle: { fontSize: 14 },
                },
                {
                    text: "Termination Types",
                    left: "78%",
                    top: 0,
                    textAlign: "center",
                    textStyle: { fontSize: 14 },
                },
            ],
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
                formatter: function (params) {
                    if (!Array.isArray(params)) {
                        // Pie tooltip
                        return params.name + ": " + params.value + "%";
                    }
                    var header = params[0].axisValueLabel;
                    var lines = params.map(function (p) {
                        return p.marker + " " + p.seriesName + ": " + p.value.toFixed(1) + "s";
                    });
                    return header + "<br/>" + lines.join("<br/>");
                },
            },
            legend: {
                data: ["Green", "Yellow", "Red", "Gap Out", "Force Off", "Max Out"],
                bottom: 0,
            },
            grid: { left: 50, right: "45%", top: 40, bottom: 40 },
            xAxis: {
                type: "category",
                data: phases,
                axisLabel: { rotate: 0 },
            },
            yAxis: {
                type: "value",
                name: "Seconds",
                nameLocation: "center",
                nameGap: 35,
            },
            series: [
                {
                    name: "Green",
                    type: "bar",
                    stack: "split",
                    data: greens,
                    itemStyle: { color: "#4ade80" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Yellow",
                    type: "bar",
                    stack: "split",
                    data: yellows,
                    itemStyle: { color: "#fbbf24" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Red",
                    type: "bar",
                    stack: "split",
                    data: reds,
                    itemStyle: { color: "#ef4444" },
                    emphasis: { focus: "series" },
                },
                {
                    name: "Termination",
                    type: "pie",
                    radius: ["30%", "55%"],
                    center: ["78%", "50%"],
                    data: pieData,
                    label: {
                        formatter: "{b}\n{d}%",
                        fontSize: 11,
                    },
                    itemStyle: {
                        borderRadius: 4,
                        borderColor: "#fff",
                        borderWidth: 2,
                    },
                    color: ["#34d399", "#f97316", "#a855f7"],
                    tooltip: {
                        trigger: "item",
                        formatter: function (p) {
                            return p.name + ": " + p.value + "%";
                        },
                    },
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.splitMonitor = splitMonitor;
})();
