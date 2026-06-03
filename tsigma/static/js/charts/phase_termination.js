/**
 * TSIGMA - Phase Termination Chart
 *
 * Stacked bar chart showing gap out, max out, force off, and skip counts
 * per time bin, with one sub-chart per phase using ECharts grid layout.
 */
(function () {
    "use strict";

    window.tsigma = window.tsigma || {};
    window.tsigma.charts = window.tsigma.charts || {};

    var phaseTermination = {};
    var _chart = null;
    var _lastData = null;
    var _themeBound = false;
    var MAX_GRID_PHASES = 8;

    phaseTermination.init = function (containerId) {
        var el = document.getElementById(containerId);
        if (!el) throw new Error("Phase termination container not found: " + containerId);
        _chart = echarts.init(el);
        tsigma.charts.resize(_chart);
        if (!_themeBound) {
            _themeBound = true;
            tsigma.theme.onChange(function () {
                if (_lastData !== null) phaseTermination.render(_lastData);
            });
        }
        return _chart;
    };

    phaseTermination.render = function (data) {
        _lastData = data;
        var T = tsigma.theme.tokens();
        if (!_chart) throw new Error("Phase termination chart not initialized");
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

        // Group data by phase
        var phaseMap = {};
        data.forEach(function (d) {
            var p = d.phase_number;
            if (!phaseMap[p]) phaseMap[p] = [];
            phaseMap[p].push(d);
        });

        var phaseNumbers = Object.keys(phaseMap).sort(function (a, b) {
            return Number(a) - Number(b);
        });

        var visiblePhases = phaseNumbers.slice(0, MAX_GRID_PHASES);
        var cols = visiblePhases.length <= 2 ? 1 : 2;
        var rows = Math.ceil(visiblePhases.length / cols);

        var grids = [];
        var xAxes = [];
        var yAxes = [];
        var series = [];
        var titles = [];
        var dataZooms = [];

        var marginLeft = 60;
        var marginRight = 20;
        var marginTop = 50;
        var marginBottom = 60;
        var gapX = 60;
        var gapY = 70;

        var cellW = (100 - 10) / cols;
        var cellH = (100 - 15) / rows;

        visiblePhases.forEach(function (phaseNum, idx) {
            var col = idx % cols;
            var row = Math.floor(idx / cols);
            var left = 5 + col * cellW + "%";
            var top = 8 + row * cellH + "%";
            var width = (cellW - 5) + "%";
            var height = (cellH - 8) + "%";

            grids.push({ left: left, top: top, width: width, height: height });

            var phaseData = phaseMap[phaseNum];
            var bins = phaseData.map(function (d) { return d.bin_start; });

            xAxes.push({
                type: "category",
                gridIndex: idx,
                data: bins,
                axisLabel: {
                    fontSize: 10,
                    rotate: 45,
                    formatter: function (v) {
                        var d = new Date(v);
                        return (d.getHours() < 10 ? "0" : "") + d.getHours() + ":" +
                               (d.getMinutes() < 10 ? "0" : "") + d.getMinutes();
                    },
                },
            });

            yAxes.push({
                type: "value",
                gridIndex: idx,
                name: "Count",
                nameTextStyle: { fontSize: 10 },
            });

            titles.push({
                text: "Phase " + phaseNum,
                left: left,
                top: (8 + row * cellH - 3) + "%",
                textStyle: { fontSize: 12 },
            });

            var seriesDefs = [
                { name: "Gap Out", key: "gap_out_count", color: T.phaseGreen },
                { name: "Max Out", key: "max_out_count", color: T.phaseRed },
                { name: "Force Off", key: "force_off_count", color: T.phaseYellow },
                { name: "Skip", key: "skip_count", color: T.mutedForeground },
            ];

            seriesDefs.forEach(function (sd) {
                series.push({
                    name: sd.name,
                    type: "bar",
                    stack: "term_" + phaseNum,
                    xAxisIndex: idx,
                    yAxisIndex: idx,
                    data: phaseData.map(function (d) { return d[sd.key] || 0; }),
                    itemStyle: { color: sd.color },
                    emphasis: { focus: "series" },
                });
            });

            dataZooms.push(
                { type: "inside", xAxisIndex: idx }
            );
        });

        // Single slider dataZoom linked to all x axes
        var allXIndices = visiblePhases.map(function (_, i) { return i; });
        dataZooms.push({
            type: "slider",
            xAxisIndex: allXIndices,
            bottom: 5,
        });

        var option = {
            title: titles,
            tooltip: {
                trigger: "axis",
                axisPointer: { type: "shadow" },
            },
            legend: {
                data: ["Gap Out", "Max Out", "Force Off", "Skip"],
                top: 0,
                left: "center",
            },
            grid: grids,
            xAxis: xAxes,
            yAxis: yAxes,
            dataZoom: dataZooms,
            series: series,
        };

        _chart.setOption(option, true);
    };

    window.tsigma.charts.phaseTermination = phaseTermination;
})();
