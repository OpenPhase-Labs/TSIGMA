/**
 * TSIGMA - Red Light Monitor Chart
 *
 * Scatter plot of red light violation counts per cycle, with color
 * coding for violation vs clean cycles and summary statistics.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var redLightMonitor = {};
    var _chart = null;

    redLightMonitor.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Red light monitor container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    redLightMonitor.render = function (data) {
        if (!_chart) throw new Error("Red light monitor chart not initialized");
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

        var totalCycles = sorted.length;
        var totalViolations = sorted.reduce(function (s, d) {
            return s + (d.violation_count || 0);
        }, 0);
        var cyclesWithViolations = sorted.filter(function (d) {
            return (d.violation_count || 0) > 0;
        }).length;
        var violationPct = totalCycles > 0
            ? ((cyclesWithViolations / totalCycles) * 100).toFixed(1)
            : "0.0";

        var violationData = [];
        var cleanData = [];

        sorted.forEach(function (d) {
            var point = [d.cycle_start, d.violation_count || 0];
            if ((d.violation_count || 0) > 0) {
                violationData.push(point);
            } else {
                cleanData.push(point);
            }
        });

        var option = {
            title: [
                {
                    text: "Red Light Violation Monitor",
                    left: "center",
                    textStyle: { fontSize: 14 },
                },
                {
                    text: "Total Violations: " + totalViolations +
                          "  |  Cycles with Violations: " + cyclesWithViolations +
                          "/" + totalCycles + " (" + violationPct + "%)",
                    left: "center",
                    top: 25,
                    textStyle: { fontSize: 11, color: "#6b7280", fontWeight: "normal" },
                },
            ],
            tooltip: {
                trigger: "item",
                formatter: function (params) {
                    var dt = new Date(params.value[0]);
                    var time = tsigma.dates
                        ? tsigma.dates.formatDisplay(dt.toISOString())
                        : dt.toLocaleString();
                    var count = params.value[1];
                    var lines = [time, "Violations: " + count];

                    // Find matching data entry for violation timestamps
                    if (count > 0) {
                        var match = sorted.find(function (d) {
                            return d.cycle_start === params.value[0];
                        });
                        if (match && match.violations && match.violations.length > 0) {
                            lines.push("");
                            lines.push("Violation times:");
                            match.violations.forEach(function (v) {
                                var vt = new Date(v);
                                lines.push("  " + (tsigma.dates
                                    ? tsigma.dates.formatDisplay(vt.toISOString())
                                    : vt.toLocaleTimeString()));
                            });
                        }
                    }
                    return lines.join("<br/>");
                },
            },
            legend: {
                data: ["Violation Cycles", "Clean Cycles"],
                bottom: 0,
            },
            grid: { left: 60, right: 30, top: 55, bottom: 70 },
            xAxis: {
                type: "time",
                name: "Time",
                nameLocation: "center",
                nameGap: 30,
            },
            yAxis: {
                type: "value",
                name: "Violation Count",
                nameLocation: "center",
                nameGap: 40,
                minInterval: 1,
            },
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: [
                {
                    name: "Violation Cycles",
                    type: "scatter",
                    data: violationData,
                    symbolSize: 8,
                    itemStyle: { color: "#ef4444" },
                },
                {
                    name: "Clean Cycles",
                    type: "scatter",
                    data: cleanData,
                    symbolSize: 5,
                    itemStyle: { color: "#9ca3af", opacity: 0.5 },
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.redLightMonitor = redLightMonitor;
})();
