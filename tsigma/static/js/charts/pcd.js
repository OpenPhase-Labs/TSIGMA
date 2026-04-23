/**
 * TSIGMA - Purdue Coordination Diagram (PCD)
 *
 * Renders cycle-level timing bands (green / yellow / red) with
 * detector activation scatter points overlaid.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var pcd = {};
    var _chart = null;

    /**
     * Initialize an ECharts instance inside the given container.
     * @param {string} containerId
     * @returns {echarts.ECharts}
     */
    pcd.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("PCD container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    /**
     * Render the Purdue Coordination Diagram.
     *
     * @param {Array<Object>} data  List of cycle objects from the
     *   purdue-diagram report endpoint.
     */
    pcd.render = function (data) {
        if (!_chart) throw new Error("PCD chart not initialized");
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

        // Pre-compute helper values per cycle
        var greenBands = [];
        var yellowBands = [];
        var redBands = [];
        var detectorPoints = [];

        data.forEach(function (cycle) {
            var cs = new Date(cycle.cycle_start).getTime();
            var gs = new Date(cycle.green_start).getTime();
            var ys = new Date(cycle.yellow_start).getTime();
            var rs = new Date(cycle.red_start).getTime();
            var ce = new Date(cycle.cycle_end).getTime();
            var cycleLen = (ce - cs) / 1000; // seconds

            var greenDur = (ys - gs) / 1000;
            var yellowDur = (rs - ys) / 1000;

            // Bands stored as [cycleIndex, start_sec, end_sec]
            greenBands.push({
                start: cs,
                low: 0,
                high: greenDur,
            });
            yellowBands.push({
                start: cs,
                low: greenDur,
                high: greenDur + yellowDur,
            });
            redBands.push({
                start: cs,
                low: greenDur + yellowDur,
                high: cycleLen,
            });

            // Detector activations as scatter
            if (cycle.detector_activations) {
                cycle.detector_activations.forEach(function (ts) {
                    var t = new Date(ts).getTime();
                    var offset = (t - cs) / 1000;
                    if (offset >= 0 && offset <= cycleLen) {
                        detectorPoints.push([cs, offset]);
                    }
                });
            }
        });

        // Build band series using custom renderItem
        function makeBandSeries(name, bands, color) {
            return {
                name: name,
                type: "custom",
                renderItem: function (params, api) {
                    var idx = params.dataIndex;
                    var band = bands[idx];
                    var xStart = api.coord([band.start, band.low]);
                    var xEnd = api.coord([band.start, band.high]);
                    // band width = approximate cycle duration in pixels
                    var nextX;
                    if (idx < bands.length - 1) {
                        nextX = api.coord([bands[idx + 1].start, 0])[0];
                    } else {
                        // Use cycle_end for last band
                        var lastCycle = data[idx];
                        var ce = new Date(lastCycle.cycle_end).getTime();
                        nextX = api.coord([ce, 0])[0];
                    }
                    var width = Math.max(nextX - xStart[0], 2);

                    return {
                        type: "rect",
                        shape: {
                            x: xStart[0],
                            y: Math.min(xStart[1], xEnd[1]),
                            width: width,
                            height: Math.abs(xEnd[1] - xStart[1]),
                        },
                        style: {
                            fill: color,
                        },
                    };
                },
                data: bands.map(function (b) {
                    return [b.start, b.low, b.high];
                }),
                encode: { x: 0, y: [1, 2] },
                z: 1,
            };
        }

        var option = {
            title: {
                text: "Purdue Coordination Diagram",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "item",
                formatter: function (params) {
                    if (params.seriesName === "Detector Activations") {
                        var dt = new Date(params.value[0]);
                        return (
                            "Activation<br/>" +
                            tsigma.dates.formatDisplay(dt.toISOString()) +
                            "<br/>Offset: " +
                            params.value[1].toFixed(1) +
                            "s"
                        );
                    }
                    return params.seriesName;
                },
            },
            xAxis: {
                type: "time",
                name: "Time",
                nameLocation: "center",
                nameGap: 30,
            },
            yAxis: {
                type: "value",
                name: "Cycle Offset (s)",
                nameLocation: "center",
                nameGap: 40,
                min: 0,
            },
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 10 },
                { type: "inside", xAxisIndex: 0 },
            ],
            grid: { left: 60, right: 30, top: 50, bottom: 70 },
            series: [
                makeBandSeries("Green", greenBands, "#4ade80"),
                makeBandSeries("Yellow", yellowBands, "#fbbf24"),
                makeBandSeries("Red", redBands, "#ef4444"),
                {
                    name: "Detector Activations",
                    type: "scatter",
                    data: detectorPoints,
                    symbolSize: 5,
                    itemStyle: { color: "#1e3a5f" },
                    z: 10,
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.pcd = pcd;
})();
