/**
 * tsigma.theme — bridges the CSS design-token system to canvas/WebGL widgets
 * (ECharts, MapLibre) that can't read CSS variables directly.
 *
 * The page's light/dark + per-agency colors live in CSS custom properties
 * (injected by tsigma/theming/tokens.py and switched via [data-mode]).
 * Charts and maps read the *computed* values through `tokens()` and re-apply
 * themselves when the `tsigma:themechange` event fires (dispatched by the nav
 * toggle in base.html).
 *
 * No build step — loaded as a plain <script> after echarts. See the
 * themable-ui-redesign design spec (§5, §9).
 */
(function () {
  window.tsigma = window.tsigma || {};

  // Charts registered for automatic re-theming. Each entry is
  // { chart: <ECharts instance>, render: function(tokens) }.
  var _charts = [];
  // Arbitrary subscribers (e.g. MapLibre paint updates).
  var _subscribers = [];

  function cssVar(name) {
    return getComputedStyle(document.documentElement)
      .getPropertyValue(name)
      .trim();
  }

  /** Snapshot of the active semantic + phase tokens as resolved hex/colors. */
  function tokens() {
    return {
      surface: cssVar('--color-surface'),
      surfaceRaised: cssVar('--color-surface-raised'),
      foreground: cssVar('--color-foreground'),
      mutedForeground: cssVar('--color-muted-foreground'),
      border: cssVar('--color-border'),
      ring: cssVar('--color-ring'),
      brand: cssVar('--color-brand'),
      brandForeground: cssVar('--color-brand-foreground'),
      success: cssVar('--color-success'),
      warning: cssVar('--color-warning'),
      error: cssVar('--color-error'),
      phaseGreen: cssVar('--chart-phase-green'),
      phaseYellow: cssVar('--chart-phase-yellow'),
      phaseRed: cssVar('--chart-phase-red'),
    };
  }

  /**
   * Partial ECharts option derived from tokens — spread into a chart's
   * setOption so axes/labels/tooltip track the theme. Series colors are the
   * caller's choice (use tokens() for phase-specific series).
   */
  function echartsBase() {
    var t = tokens();
    return {
      backgroundColor: 'transparent',
      textStyle: { color: t.mutedForeground },
      color: [t.brand, t.success, t.warning, t.error, t.phaseGreen, t.phaseYellow],
      tooltip: {
        backgroundColor: t.surfaceRaised,
        borderColor: t.border,
        textStyle: { color: t.foreground },
      },
    };
  }

  /** Axis style fragment (merge into an xAxis/yAxis definition). */
  function axis() {
    var t = tokens();
    return {
      axisLine: { lineStyle: { color: t.border } },
      axisTick: { lineStyle: { color: t.border } },
      axisLabel: { color: t.mutedForeground },
      splitLine: { lineStyle: { color: t.border } },
    };
  }

  /** Register the named ECharts theme 'tsigma' from current tokens. */
  function registerECharts() {
    if (!window.echarts) return;
    var t = tokens();
    echarts.registerTheme('tsigma', {
      color: [t.brand, t.success, t.warning, t.error, t.phaseGreen, t.phaseYellow],
      backgroundColor: 'transparent',
      textStyle: { color: t.mutedForeground },
      title: { textStyle: { color: t.foreground } },
      legend: { textStyle: { color: t.mutedForeground } },
      tooltip: {
        backgroundColor: t.surfaceRaised,
        borderColor: t.border,
        textStyle: { color: t.foreground },
      },
    });
  }

  /**
   * Register a chart + its render(tokens) function. Calls render once now and
   * again on every theme change. `render` should rebuild the chart's option
   * using the supplied tokens (keep the *data* stable across calls).
   */
  function register(chart, render) {
    _charts.push({ chart: chart, render: render });
    try { render(tokens()); } catch (e) { /* first paint best-effort */ }
    return chart;
  }

  /** Subscribe to theme changes; cb receives (tokens, detail). */
  function onChange(cb) {
    _subscribers.push(cb);
  }

  function _refreshAll() {
    registerECharts();
    var t = tokens();
    _charts = _charts.filter(function (c) {
      return c.chart && (!c.chart.isDisposed || !c.chart.isDisposed());
    });
    _charts.forEach(function (c) {
      try { c.render(t); } catch (e) { /* ignore a single chart's failure */ }
    });
    _subscribers.forEach(function (cb) {
      try { cb(t); } catch (e) { /* ignore */ }
    });
  }

  document.addEventListener('tsigma:themechange', _refreshAll);

  // Register the ECharts theme as soon as the script runs (echarts loads first
  // in base.html). Charts may init with echarts.init(el, 'tsigma').
  registerECharts();

  window.tsigma.theme = {
    tokens: tokens,
    echartsBase: echartsBase,
    axis: axis,
    registerECharts: registerECharts,
    register: register,
    onChange: onChange,
  };
})();
