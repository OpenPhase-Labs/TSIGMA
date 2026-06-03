/**
 * TSIGMA - Arrivals on Green (AOG) Chart
 *
 * Bar chart of AOG percentage per phase with color coding and
 * a reference line at the 70 % coordination threshold.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var arrivalsOnGreen = {};
    var _chart = null;
    var _lastData = null;
    var _themeBound = false;

    /**
     * Initialize an ECharts instance.
     * @param {string} containerId
     * @returns {echarts.ECharts}
     */
    arrivalsOnGreen.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("AOG container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        if (!_themeBound) {
            _themeBound = true;
            tsigma.theme.onChange(function () {
                if (_lastData !== null) arrivalsOnGreen.render(_lastData);
            });
        }
        return _chart;
    };

    /**
     * Return a color based on AOG percentage thresholds.
     */
    function aogColor(pct) {
        var T = tsigma.theme.tokens();
        if (pct >= 70) return T.phaseGreen; // green
        if (pct >= 40) return T.phaseYellow; // yellow/amber
        return T.phaseRed;                // red
    }

    /**
     * Render AOG data.
     * @param {Array<Object>} data  List of AOG objects per phase.
     */
    arrivalsOnGreen.render = function (data) {
        _lastData = data;
        var T = tsigma.theme.tokens();
        if (!_chart) throw new Error("AOG chart not initialized");
        if (!data || data.length === 0) {
            _chart.clear();
            _chart.setOption({
                title: {
                    text: "No data available",
                    left: "center",
                    top: "center",
                    textStyle: { color: T.mutedForeground, fontSize: 14 },
                },
            });
            return;
        }

        var phases = data.map(function (d) { return "Phase " + d.phase; });
        var pcts = data.map(function (d) { return d.aog_percentage; });
        var barColors = pcts.map(function (p) { return aogColor(p); });

        var option = {
            title: {
                text: "Arrivals on Green",
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
                        "<br/>AOG: " + d.aog_percentage.toFixed(1) + "%" +
                        "<br/>Arrivals on Green: " + d.arrivals_on_green +
                        "<br/>Total Arrivals: " + d.total_arrivals
                    );
                },
            },
            grid: { left: 50, right: 30, top: 50, bottom: 40 },
            xAxis: {
                type: "category",
                data: phases,
            },
            yAxis: {
                type: "value",
                name: "AOG %",
                nameLocation: "center",
                nameGap: 35,
                min: 0,
                max: 100,
            },
            series: [
                {
                    name: "AOG %",
                    type: "bar",
                    data: pcts.map(function (v, i) {
                        return { value: v, itemStyle: { color: barColors[i] } };
                    }),
                    label: {
                        show: true,
                        position: "top",
                        formatter: function (params) {
                            var d = data[params.dataIndex];
                            return d.arrivals_on_green + "/" + d.total_arrivals;
                        },
                        fontSize: 10,
                        color: T.border,
                    },
                    markLine: {
                        silent: true,
                        symbol: "none",
                        lineStyle: { type: "dashed", color: T.phaseGreen, width: 2 },
                        data: [
                            {
                                yAxis: 70,
                                label: {
                                    formatter: "70% threshold",
                                    position: "end",
                                    fontSize: 10,
                                },
                            },
                        ],
                    },
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.arrivalsOnGreen = arrivalsOnGreen;
})();
