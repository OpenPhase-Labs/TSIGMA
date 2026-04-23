# TSIGMA UI Quick Start Guide

**Purpose**: Get the TSIGMA web UI running.

**Last Updated**: 2026-03-03

---

## Start TSIGMA

```bash
cd TSIGMA

# 1. Install dependencies
pip install -e ".[dev]"

# 2. Setup database
createdb tsigma
psql tsigma -c "CREATE EXTENSION timescaledb;"

# 3. Apply migrations
alembic upgrade head

# 4. Start server
uvicorn tsigma.app:app --host 0.0.0.0 --port 8080 --reload
```

---

## Access UI Pages

| Page | URL | Description |
|------|-----|-------------|
| **Dashboard** | http://localhost:8080 | Main dashboard with quick stats |
| **PCD** | http://localhost:8080/analytics/pcd | Purdue Coordination Diagram |
| **Volume** | http://localhost:8080/analytics/volume | Traffic volume analysis |
| **Delay** | http://localhost:8080/analytics/delay | Approach delay |
| **Speed** | http://localhost:8080/analytics/speed | Speed distribution |
| **Health** | http://localhost:8080/health | Signal health dashboard |
| **API Docs** | http://localhost:8080/docs | OpenAPI documentation (Swagger UI) |

---

## UI Features

### Islands Architecture

**No build step required!**

- ✅ FastAPI renders base HTML (Jinja2 templates)
- ✅ HTMX loads chart data dynamically (no page reload)
- ✅ Alpine.js handles client-side state (date pickers, filters)
- ✅ TailwindCSS for styling (utility classes)
- ✅ ECharts for visualizations (interactive charts)

**All self-hosted** - No npm, no webpack, no external CDN dependencies!

---

## Interactive Features

### Dashboard (`/`)

- Signal selector dropdown
- Metric selector (Volume, Delay, Speed, Arrivals, Splits)
- Date range picker
- Dynamic chart loading (changes without page reload)
- Quick stats (volume, delay, arrivals on green, health)

### PCD Page (`/analytics/pcd`)

- Date/time picker (15 min to 24 hours)
- Phase selector (1-16)
- Quick time range buttons (15 min, 1 hour, 4 hours, 24 hours)
- Interactive PCD scatter plot (ECharts)
- Real-time metrics (PAoG, platoon ratio, arrivals, cycles)

### Volume Page (`/analytics/volume`)

- View mode toggle (Hourly / 15-Minute / Daily)
- Volume bar chart (ECharts)
- Metrics: Total volume, peak hour, PHF

### Delay Page (`/analytics/delay`)

- Approach selector (NB, SB, EB, WB)
- 15-minute delay chart (reads from continuous aggregate)
- Multi-line chart (Average, 85th, 95th percentile)
- Metrics: Avg delay, vehicles delayed, % delayed

### Speed Page (`/analytics/speed`)

- Detector selector
- Posted speed limit input
- Speed histogram (5 bins: <25, 25-35, 35-45, 45-55, >55 mph)
- Posted speed overlay (dashed line)
- Metrics: 15th, 50th, 85th percentile speeds

### Health Dashboard (`/health`)

- Status filter (All, Critical, Poor, Good)
- Sort by (Health score, Name, Alert count)
- Signal health table with:
  - Health score (0-100)
  - Status badge (color-coded)
  - Detector health
  - Communication status
  - Active issues list
  - Quick action links

---

## HTMX Examples

### Update Chart on Date Change

```html
<!-- Date picker triggers HTMX request on change -->
<input type="datetime-local" x-model="startDate">

<!-- Chart reloads when input changes -->
<div hx-get="/api/v1/ui/fragments/pcd_chart"
     hx-trigger="change from:input delay:500ms"
     hx-vals="js:{start: startDate}">
    Loading...
</div>
```

**Result**: Chart updates without page reload (feels like SPA)

### Filter Table on Button Click

```html
<!-- Filter buttons (Alpine.js state) -->
<button @click="filter = 'critical'"
        :class="filter === 'critical' ? 'active' : ''">
    Critical
</button>

<!-- Table reloads when filter changes -->
<div hx-get="/api/v1/ui/fragments/health_table"
     hx-trigger="change from:button"
     hx-vals="js:{filter: filter}">
    Loading...
</div>
```

**Result**: Table filters instantly (no page reload)

---

## Alpine.js Examples

### Date Range State

```html
<div x-data="{
    startDate: '2026-03-03',
    endDate: '2026-03-04'
}">
    <input type="date" x-model="startDate">
    <input type="date" x-model="endDate">

    <!-- Display -->
    <p x-text="'Range: ' + startDate + ' to ' + endDate"></p>
</div>
```

### Modal Dialog

```html
<div x-data="{ open: false }">
    <button @click="open = true">Open Filter</button>

    <div x-show="open" x-transition class="modal">
        <h2>Filter Options</h2>
        <button @click="open = false">Close</button>
    </div>
</div>
```

---

## ECharts Examples

### Bar Chart (Volume)

```javascript
const chart = echarts.init(document.getElementById('volume-chart'));
chart.setOption({
    xAxis: { type: 'category', data: ['06:00', '07:00', '08:00'] },
    yAxis: { type: 'value', name: 'Vehicles' },
    series: [{
        type: 'bar',
        data: [450, 650, 580],
        itemStyle: { color: '#3b82f6' }
    }]
});
```

### Line Chart (Delay)

```javascript
chart.setOption({
    xAxis: { type: 'time' },
    yAxis: { type: 'value', name: 'Delay (seconds)' },
    series: [{
        name: 'Average Delay',
        type: 'line',
        data: [[timestamp1, 15.3], [timestamp2, 18.2], ...],
        smooth: true
    }]
});
```

---

## Performance

| Metric | Value | Notes |
|--------|-------|-------|
| **Initial Page Load** | 300-800ms | Server-rendered HTML |
| **Chart Update (HTMX)** | 500ms-1s | Fetch fragment + render |
| **Client-side Toggle** | Instant | Alpine.js (no server) |
| **Bundle Size** | ~50KB | HTMX + Alpine.js (vs 500KB+ for React) |

---

## Development Workflow

### Add New Chart

1. Create template: `tsigma/templates/analytics/new_metric.html`
2. Create fragment: `tsigma/templates/fragments/new_metric_chart.html`
3. Add route: `@router.get("/analytics/new_metric")`
4. Add fragment endpoint: `@router.get("/api/v1/ui/fragments/new_metric_chart")`
5. Add nav link: Update `base.html` nav

**No build step!** Just refresh browser.

---

**TSIGMA UI is production-ready** with 6 pages, 7 chart types, and full Islands Architecture!

---

**Document Version**: 1.0
**Last Updated**: 2026-03-03
**Owner**: OpenPhase Labs
