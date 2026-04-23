/**
 * TSIGMA - Time-Space Diagram Chart
 *
 * Custom-rendered diagram with horizontal phase-state bars per signal
 * (green/yellow/red) and diagonal dashed speed reference lines.
 * Y-axis = distance (feet), X-axis = time.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var timeSpaceDiagram = {};
    var _chart = null;

    var STATE_COLORS = {
        green: "#22c55e",
        yellow: "#eab308",
        red: "#ef4444",
    };

    var BAR_HEIGHT_PX = 14;

    timeSpaceDiagram.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Time-space diagram container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    timeSpaceDiagram.render = function (data) {
        if (!_chart) throw new Error("Time-space diagram chart not initialized");
        if (!data || !data.signals || data.signals.length === 0) {
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

        var signals = data.signals;
        var speedLines = data.speed_lines || [];

        // Compute time range across all phase intervals
        var timeMin = Infinity;
        var timeMax = -Infinity;
        var distMin = Infinity;
        var distMax = -Infinity;

        signals.forEach(function (sig) {
            var d = sig.distance || 0;
            if (d < distMin) distMin = d;
            if (d > distMax) distMax = d;
            (sig.phase_intervals || []).forEach(function (pi) {
                var s = new Date(pi.start).getTime();
                var e = new Date(pi.end).getTime();
                if (s < timeMin) timeMin = s;
                if (e > timeMax) timeMax = e;
            });
        });

        // Build custom series data: one entry per phase interval
        var phaseData = [];
        signals.forEach(function (sig) {
            (sig.phase_intervals || []).forEach(function (pi) {
                phaseData.push({
                    signal_id: sig.signal_id,
                    distance: sig.distance || 0,
                    start: new Date(pi.start).getTime(),
                    end: new Date(pi.end).getTime(),
                    state: pi.state,
                });
            });
        });

        // Encode as array values: [start, distance, end, stateIndex, dataIndex]
        var stateList = ["green", "yellow", "red"];
        var customData = phaseData.map(function (d, i) {
            var si = stateList.indexOf(d.state);
            if (si === -1) si = 2; // default to red for unknown
            return [d.start, d.distance, d.end, si, i];
        });

        // Build speed line series
        var speedSeries = speedLines.map(function (sl) {
            var lineData = (sl.points || []).map(function (pt) {
                return [new Date(pt.time).getTime(), pt.distance];
            });
            return {
                name: sl.speed_mph + " mph",
                type: "line",
                data: lineData,
                lineStyle: { type: "dashed", width: 1.5, color: "#6b7280" },
                itemStyle: { color: "#6b7280" },
                symbol: "none",
                z: 5,
            };
        });

        var legendData = speedSeries.map(function (s) { return s.name; });
        legendData.unshift("Phase Intervals");

        var distPadding = (distMax - distMin) * 0.05 || 50;

        var option = {
            title: {
                text: "Time-Space Diagram",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "item",
                formatter: function (params) {
                    if (params.componentSubType === "custom") {
                        var idx = params.value[4];
                        var d = phaseData[idx];
                        if (!d) return "";
                        var startDt = new Date(d.start);
                        var endDt = new Date(d.end);
                        var fmt = function (dt) {
                            return tsigma.dates
                                ? tsigma.dates.formatDisplay(dt.toISOString())
                                : dt.toLocaleTimeString();
                        };
                        return (
                            "Signal: " + d.signal_id + "<br/>" +
                            "Distance: " + d.distance + " ft<br/>" +
                            "State: " + d.state + "<br/>" +
                            fmt(startDt) + " - " + fmt(endDt)
                        );
                    }
                    return params.seriesName;
                },
            },
            legend: {
                data: legendData,
                top: 30,
            },
            grid: { left: 80, right: 40, top: 70, bottom: 70 },
            xAxis: {
                type: "time",
                name: "Time",
                nameLocation: "center",
                nameGap: 30,
                min: timeMin,
                max: timeMax,
            },
            yAxis: {
                type: "value",
                name: "Distance (ft)",
                nameLocation: "center",
                nameGap: 55,
                min: distMin - distPadding,
                max: distMax + distPadding,
            },
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: [
                {
                    name: "Phase Intervals",
                    type: "custom",
                    renderItem: function (params, api) {
                        var startVal = api.value(0);
                        var dist = api.value(1);
                        var endVal = api.value(2);
                        var stateIdx = api.value(3);

                        var startPt = api.coord([startVal, dist]);
                        var endPt = api.coord([endVal, dist]);

                        var color = STATE_COLORS[stateList[stateIdx]] || STATE_COLORS.red;
                        var halfH = BAR_HEIGHT_PX / 2;

                        var rectShape = echarts.graphic.clipRectByRect(
                            {
                                x: startPt[0],
                                y: startPt[1] - halfH,
                                width: endPt[0] - startPt[0],
                                height: BAR_HEIGHT_PX,
                            },
                            {
                                x: params.coordSys.x,
                                y: params.coordSys.y,
                                width: params.coordSys.width,
                                height: params.coordSys.height,
                            }
                        );

                        if (rectShape) {
                            return {
                                type: "rect",
                                shape: rectShape,
                                style: api.style({
                                    fill: color,
                                    stroke: "#374151",
                                    lineWidth: 0.5,
                                }),
                            };
                        }
                    },
                    data: customData,
                    encode: {
                        x: [0, 2],
                        y: 1,
                    },
                    z: 10,
                },
            ].concat(speedSeries),
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.timeSpaceDiagram = timeSpaceDiagram;
})();
