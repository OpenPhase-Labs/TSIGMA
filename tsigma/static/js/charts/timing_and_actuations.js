/**
 * TSIGMA - Timing and Actuations Chart
 *
 * Timeline/swimlane chart showing phase state bands (green/yellow/red)
 * for ALL phases simultaneously with detector actuation tick marks overlaid.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var timingAndActuations = {};
    var _chart = null;

    // NTCIP 1202 phase event codes
    var PHASE_GREEN = 1;
    var PHASE_YELLOW = 8;
    var PHASE_RED = 10;
    var DETECTOR_ON = 81;

    var STATE_COLORS = {
        green: "#4ade80",
        yellow: "#fbbf24",
        red: "#ef4444",
    };

    timingAndActuations.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Timing and actuations container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        return _chart;
    };

    timingAndActuations.render = function (data) {
        if (!_chart) throw new Error("Timing and actuations chart not initialized");
        if (!data || (!data.events && !data.phase_summary)) {
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

        var events = data.events || [];
        if (events.length === 0) {
            _chart.clear();
            _chart.setOption({
                title: {
                    text: "No event data available",
                    left: "center",
                    top: "center",
                    textStyle: { color: "#9ca3af", fontSize: 14 },
                },
            });
            return;
        }

        // Sort events by time
        events.sort(function (a, b) {
            return new Date(a.event_time) - new Date(b.event_time);
        });

        // Build phase state intervals from events
        // Event codes: 1=green on, 7=green hold, 8=yellow on, 10=red on, 11=red clearance
        var phaseGreenCodes = [1, 7];
        var phaseYellowCodes = [8, 9];
        var phaseRedCodes = [10, 11];

        var phaseStateEvents = {};  // phase -> [{time, state}]
        var detectorEvents = [];    // [{time, phase}]
        var phases = {};

        events.forEach(function (e) {
            var code = e.event_code;
            var param = e.event_param;
            var time = new Date(e.event_time).getTime();

            if (phaseGreenCodes.indexOf(code) >= 0) {
                if (!phaseStateEvents[param]) phaseStateEvents[param] = [];
                phaseStateEvents[param].push({ time: time, state: "green" });
                phases[param] = true;
            } else if (phaseYellowCodes.indexOf(code) >= 0) {
                if (!phaseStateEvents[param]) phaseStateEvents[param] = [];
                phaseStateEvents[param].push({ time: time, state: "yellow" });
                phases[param] = true;
            } else if (phaseRedCodes.indexOf(code) >= 0) {
                if (!phaseStateEvents[param]) phaseStateEvents[param] = [];
                phaseStateEvents[param].push({ time: time, state: "red" });
                phases[param] = true;
            } else if (code === DETECTOR_ON) {
                detectorEvents.push({ time: time, detector: param });
            }
        });

        var phaseList = Object.keys(phases).sort(function (a, b) {
            return Number(a) - Number(b);
        });

        var phaseLabels = phaseList.map(function (p) { return "Phase " + p; });

        // Build band data for custom renderItem
        // Each band: [phaseIndex, startTime, endTime, state]
        var bandData = [];
        phaseList.forEach(function (phase, phaseIdx) {
            var stateList = phaseStateEvents[phase] || [];
            stateList.sort(function (a, b) { return a.time - b.time; });

            for (var i = 0; i < stateList.length; i++) {
                var start = stateList[i].time;
                var end = (i < stateList.length - 1) ? stateList[i + 1].time : start + 5000;
                var state = stateList[i].state;
                bandData.push([phaseIdx, start, end, state]);
            }
        });

        // Map detector events to nearest phase for Y positioning
        // Detectors overlay at bottom of chart
        var detectorData = detectorEvents.map(function (d) {
            return [d.time, d.detector];
        });

        var option = {
            title: {
                text: "Timing and Actuations",
                left: "center",
                textStyle: { fontSize: 14 },
            },
            tooltip: {
                trigger: "item",
                formatter: function (params) {
                    if (params.seriesName === "Detector Actuations") {
                        var dt = new Date(params.value[0]);
                        var time = tsigma.dates
                            ? tsigma.dates.formatDisplay(dt.toISOString())
                            : dt.toLocaleString();
                        return "Detector " + params.value[1] + "<br/>" + time;
                    }
                    if (params.value && params.value.length >= 4) {
                        var startDt = new Date(params.value[1]);
                        var endDt = new Date(params.value[2]);
                        var dur = ((params.value[2] - params.value[1]) / 1000).toFixed(1);
                        var phase = phaseLabels[params.value[0]];
                        return phase + " - " + params.value[3].toUpperCase() + "<br/>" +
                               dur + "s";
                    }
                    return "";
                },
            },
            legend: {
                data: ["Green", "Yellow", "Red", "Detector Actuations"],
                bottom: 0,
            },
            grid: { left: 80, right: 30, top: 50, bottom: 70 },
            xAxis: {
                type: "time",
                name: "Time",
                nameLocation: "center",
                nameGap: 30,
            },
            yAxis: {
                type: "category",
                data: phaseLabels,
                axisLabel: { fontSize: 11 },
                inverse: true,
            },
            dataZoom: [
                { type: "slider", xAxisIndex: 0, bottom: 25 },
                { type: "inside", xAxisIndex: 0 },
            ],
            series: [
                {
                    name: "Phase States",
                    type: "custom",
                    renderItem: function (params, api) {
                        var phaseIdx = api.value(0);
                        var startTime = api.value(1);
                        var endTime = api.value(2);
                        var state = api.value(3);

                        var start = api.coord([startTime, phaseIdx]);
                        var end = api.coord([endTime, phaseIdx]);
                        var bandHeight = api.size([0, 1])[1] * 0.7;

                        return {
                            type: "rect",
                            shape: {
                                x: start[0],
                                y: start[1] - bandHeight / 2,
                                width: Math.max(end[0] - start[0], 1),
                                height: bandHeight,
                            },
                            style: {
                                fill: STATE_COLORS[state] || "#9ca3af",
                            },
                        };
                    },
                    data: bandData,
                    encode: {
                        x: [1, 2],
                        y: 0,
                    },
                    z: 1,
                },
                {
                    name: "Detector Actuations",
                    type: "scatter",
                    data: detectorData,
                    symbolSize: [2, 12],
                    symbol: "rect",
                    itemStyle: { color: "#1e3a5f", opacity: 0.6 },
                    z: 10,
                },
            ],
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.timingAndActuations = timingAndActuations;
})();
