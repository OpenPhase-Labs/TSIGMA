# TSIGMA UI Architecture

**Purpose**: Document TSIGMA's zero-build, vendor-downloaded web UI architecture.

**Last Updated**: 2026-03-05

---

## Design Philosophy

**✅ What We Want:**
- Fast initial load (server-rendered HTML)
- Interactive charts and dashboards (ECharts, MapLibre)
- Zero build step (no npm, no webpack, no bundlers)
- Air-gapped compatible (all libraries vendor-downloaded and committed)
- Efficient chart updates (vanilla JS updates data without destroying charts)
- Progressive enhancement (basic HTML works, JS enhances)

**❌ What We DON'T Want:**
- Full SPA (React, Vue, Angular) - Overkill, requires build step
- CDN dependencies - Breaks air-gapped deployments
- npm/Node.js - No package.json, no node_modules
- Complex build tooling (webpack, vite) - Maintenance burden

---

## Technology Stack

### Core Technologies

```
┌─────────────────────────────────────────────────────────┐
│                    FastAPI (Server)                      │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Jinja2 Templates                                │   │
│  │  - Renders base HTML skeleton                    │   │
│  │  - Includes initial data                         │   │
│  │  - Server-rendered page structure                │   │
│  └──────────────────────────────────────────────────┘   │
└───────────────────────┬─────────────────────────────────┘
                        │ HTML Response
        ┌───────────────▼────────────────┐
        │        Browser                 │
        │  ┌─────────────────────────┐   │
        │  │  Alpine.js              │   │  ← Client-side state
        │  │  (vendor downloaded)    │   │    (dropdowns, modals)
        │  └─────────────────────────┘   │
        │  ┌─────────────────────────┐   │
        │  │  Vanilla JavaScript     │   │  ← Data fetching
        │  │  (native browser)       │   │    (JSON APIs)
        │  └─────────────────────────┘   │
        │  ┌─────────────────────────┐   │
        │  │  ECharts                │   │  ← Charts/visualization
        │  │  (vendor downloaded)    │   │    (PCD, volume, etc.)
        │  └─────────────────────────┘   │
        │  ┌─────────────────────────┐   │
        │  │  MapLibre GL JS         │   │  ← Map visualization
        │  │  (vendor downloaded)    │   │    (signal locations)
        │  └─────────────────────────┘   │
        │  ┌─────────────────────────┐   │
        │  │  Tailwind CSS           │   │  ← Utility-first styling
        │  │  (vendor downloaded)    │   │    (no custom CSS)
        │  └─────────────────────────┘   │
        │  ┌─────────────────────────┐   │
        │  │  WebSocket              │   │  ← Real-time updates
        │  │  (native browser API)   │   │    (optional, live dashboards)
        │  └─────────────────────────┘   │
        └─────────────────────────────────┘
```

---

## Component Responsibilities

### 1. FastAPI + Jinja2 (Server-Side)

**Purpose**: Render initial HTML page structure and inject data

**Use for**:
- Page layouts (header, nav, footer)
- Initial data injection (signal list, date ranges)
- Forms (signal configuration, detector setup)
- Chart container setup

**Example** (`tsigma/api/ui.py`):
```python
from pathlib import Path

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from ..auth.dependencies import require_access, require_admin

_authenticated_router = APIRouter(
    tags=["ui"],
    dependencies=[Depends(require_access("ui"))],
)

_template_dir = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(_template_dir))

@_authenticated_router.get("/signals", response_class=HTMLResponse)
async def signals_list(request: Request):
    """Signal list page."""
    return templates.TemplateResponse("pages/signals/index.html", {
        "request": request,
    })
```

The UI router is split into three sub-routers by access level:
- `_public_router` — login page (always reachable)
- `_authenticated_router` — pages governed by the `"ui"` access policy
- `_admin_router` — admin-only pages (`/admin/users`, `/admin/settings`)

**Template** (`tsigma/templates/pages/reports/viewer.html`):
```html
{% extends "base.html" %}

{% block content %}
<h1>{{ report_name }}</h1>

<!-- Signal selector populated via JS from the REST API -->
<select id="signal-select"></select>

<!-- Chart container -->
<div id="report-chart" style="width: 100%; height: 400px;"></div>

<!-- Load chart data and render -->
<script src="/static/js/charts/{{ report_name }}.js"></script>
{% endblock %}
```

All data fetching happens client-side via JavaScript calling the REST API. Jinja2 templates provide the page skeleton and route parameters.

---

### 2. Vanilla JavaScript (Data Fetching & Chart Updates)

**Purpose**: Fetch JSON data from APIs and update chart visualizations

**Use for**:
- Fetching data from REST APIs (JSON responses)
- Updating chart data without destroying/recreating charts
- Event handlers (user changes date range → fetch new data)
- Real-time updates (polling or WebSocket)

**Key Pattern**: Update chart data in-place, don't destroy and recreate

**Example**:
```javascript
// tsigma/static/js/charts/volume.js

// Initialize chart once
const chart = echarts.init(document.getElementById('volume-chart'));

// Set initial empty state
chart.setOption({
    title: { text: 'Volume Analysis' },
    xAxis: { type: 'category', data: [] },
    yAxis: { type: 'value' },
    series: [{ type: 'bar', data: [] }]
});

// Fetch data from JSON API
async function loadVolumeData(signalId, startDate, endDate) {
    const response = await fetch(`/api/v1/analytics/volume?` + new URLSearchParams({
        signal_id: signalId,
        start: startDate,
        end: endDate
    }));

    const data = await response.json();

    // Update chart data (efficient - no DOM thrashing)
    chart.setOption({
        xAxis: { data: data.bins },
        series: [{ data: data.volumes }]
    });
}

// Event listener for signal selector
document.getElementById('signal-select').addEventListener('change', (e) => {
    const selectedId = e.target.value;
    const startDate = document.getElementById('start-date').value;
    const endDate = document.getElementById('end-date').value;

    loadVolumeData(selectedId, startDate, endDate);
});

// Load initial data
loadVolumeData('default-signal-id', '2026-03-01', '2026-03-05');
```

---

### 3. Alpine.js (Client-Side State Management)

**Purpose**: Lightweight reactive UI state (no server round-trip needed)

**Use for**:
- Dropdowns (show/hide)
- Modals (open/close)
- Tabs (switch between views)
- Form validation (check required fields)
- Accordion (expand/collapse)

**Example**:
```html
<!-- Date picker with Alpine.js state -->
<div x-data="{ open: false, startDate: '2026-03-01', endDate: '2026-03-05' }">
    <button @click="open = !open" class="bg-blue-500 text-white px-4 py-2 rounded">
        Select Date Range
    </button>

    <div x-show="open" x-transition class="mt-2 p-4 bg-white shadow rounded">
        <input type="date" x-model="startDate" class="border rounded px-3 py-2">
        <input type="date" x-model="endDate" class="border rounded px-3 py-2">
        <button @click="open = false; loadChartData(startDate, endDate)"
                class="bg-green-500 text-white px-4 py-2 rounded">
            Apply
        </button>
    </div>
</div>

<!-- Tab navigation -->
<div x-data="{ tab: 'volume' }">
    <button @click="tab = 'volume'"
            :class="tab === 'volume' ? 'bg-blue-500 text-white' : 'bg-gray-200'">
        Volume
    </button>
    <button @click="tab = 'delay'"
            :class="tab === 'delay' ? 'bg-blue-500 text-white' : 'bg-gray-200'">
        Delay
    </button>

    <div x-show="tab === 'volume'" id="volume-chart-container"></div>
    <div x-show="tab === 'delay'" id="delay-chart-container"></div>
</div>
```

---

### 4. ECharts (Interactive Visualizations)

**Purpose**: Render interactive charts and handle user interactions

**Use for**:
- PCD (Purdue Coordination Diagram)
- Volume bar charts
- Speed histograms
- Delay line charts
- Occupancy heatmaps

**Key Principle**: Initialize once, update data efficiently

**Example**:
```html
<div id="pcd-chart" style="width: 100%; height: 600px;"></div>

<script>
// Initialize chart
const pcdChart = echarts.init(document.getElementById('pcd-chart'));

// Set initial configuration
pcdChart.setOption({
    title: { text: 'Purdue Coordination Diagram' },
    xAxis: { type: 'time' },
    yAxis: { type: 'value', name: 'Detector' },
    series: [
        {
            name: 'Arrivals',
            type: 'scatter',
            data: []  // Empty initially
        },
        {
            name: 'Green Band',
            type: 'line',
            data: [],
            areaStyle: { color: '#4ade80' }
        }
    ]
});

// Fetch and update data
async function loadPCDData(signalId, phase, start, end) {
    const response = await fetch(`/api/v1/analytics/pcd?` + new URLSearchParams({
        signal_id: signalId,
        phase: phase,
        start: start,
        end: end
    }));

    const data = await response.json();

    // Efficient update (doesn't recreate chart)
    pcdChart.setOption({
        series: [
            { data: data.arrivals },
            { data: data.green_band }
        ]
    });
}

// Resize chart on window resize
window.addEventListener('resize', () => pcdChart.resize());
</script>
```

---

### 5. MapLibre GL JS (Map Visualization)

**Purpose**: Interactive map for signal locations

**Use for**:
- Signal location markers
- Corridor visualization
- Real-time status indicators
- Click-to-detail navigation

**Example**:
```html
<div id="map" style="width: 100%; height: 600px;"></div>

<script>
const map = new maplibregl.Map({
    container: 'map',
    style: '/static/map/style.json',  // Self-hosted map style
    center: [-84.388, 33.749],  // Atlanta
    zoom: 10
});

map.on('load', async () => {
    // Fetch signal locations from API
    const response = await fetch('/api/v1/signals/geojson');
    const geojson = await response.json();

    // Add signals layer
    map.addSource('signals', {
        type: 'geojson',
        data: geojson
    });

    map.addLayer({
        id: 'signals',
        type: 'circle',
        source: 'signals',
        paint: {
            'circle-radius': 6,
            'circle-color': ['get', 'status_color']
        }
    });

    // Click handler
    map.on('click', 'signals', (e) => {
        const props = e.features[0].properties;
        window.location.href = `/signals/${props.id}`;
    });
});
</script>
```

---

## Vendor Library Setup

### One-Time Download and Commit

All vendor libraries are downloaded once and committed to the repository. No CDN, no npm, no build step.

**Directory structure**:
```
tsigma/static/vendor/
├── alpine/
│   ├── alpine.min.js           # Alpine.js 3.13.5
│   └── alpine.min.js.map
├── echarts/
│   ├── echarts.min.js          # ECharts 5.4.3
│   └── echarts.min.js.map
├── maplibre/
│   ├── maplibre-gl.js          # MapLibre GL JS 3.6.2
│   ├── maplibre-gl.css
│   └── maplibre-gl.js.map
└── tailwind/
    ├── tailwind.min.js         # Tailwind CSS CDN build
    └── tailwind.config.js      # Custom config
```

### Download Script

```bash
#!/bin/bash
# scripts/download_vendor_libs.sh

VENDOR_DIR="tsigma/static/vendor"

# Alpine.js
curl -o $VENDOR_DIR/alpine/alpine.min.js \
  https://cdn.jsdelivr.net/npm/alpinejs@3.13.5/dist/cdn.min.js

# ECharts
curl -o $VENDOR_DIR/echarts/echarts.min.js \
  https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js

# MapLibre GL JS
curl -o $VENDOR_DIR/maplibre/maplibre-gl.js \
  https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.js
curl -o $VENDOR_DIR/maplibre/maplibre-gl.css \
  https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.css

# Tailwind CSS (CDN build for development)
curl -o $VENDOR_DIR/tailwind/tailwind.min.js \
  https://cdn.tailwindcss.com/3.4.1
```

**Run once, commit files, never download again.**

---

## UI Patterns

### Pattern 1: Historical Analysis (User-Triggered)

**Use case**: User selects parameters, clicks "Run Analysis", sees results

**Flow**:
```
User opens page
    ↓
Server renders HTML with form (Jinja2)
    ↓
User fills in parameters (signal, date range, phase)
    ↓
User clicks "Run Analysis" button
    ↓
JavaScript fetches JSON from API (await fetch(...))
    ↓
JavaScript updates chart with new data (chart.setOption(...))
```

**Example**: PCD chart, gap analysis, split monitor

```javascript
// User clicks "Run Analysis"
document.getElementById('run-analysis').addEventListener('click', async () => {
    const signalId = document.getElementById('signal-select').value;
    const startDate = document.getElementById('start-date').value;
    const endDate = document.getElementById('end-date').value;

    // Show loading indicator
    document.getElementById('loading').style.display = 'block';

    // Fetch data from JSON API
    const response = await fetch(`/api/v1/analytics/volume?` + new URLSearchParams({
        signal_id: signalId,
        start: startDate,
        end: endDate
    }));

    const data = await response.json();

    // Update chart
    chart.setOption({
        xAxis: { data: data.bins },
        series: [{ data: data.volumes }]
    });

    // Hide loading indicator
    document.getElementById('loading').style.display = 'none';
});
```

---

### Pattern 2: Live Dashboard (Polling or WebSocket)

**Use case**: Dashboard shows real-time signal status, auto-updates every 30 seconds

**Flow (Polling)**:
```
User opens dashboard
    ↓
Server renders HTML with empty chart containers (Jinja2)
    ↓
JavaScript initializes charts (echarts.init(...))
    ↓
JavaScript starts polling (setInterval(...))
    ↓
Every 30 seconds:
    Fetch latest data from API (await fetch(...))
    Update chart data (chart.setOption(...))
```

**Example**: Live signal health dashboard

```javascript
// Initialize charts
const statusChart = echarts.init(document.getElementById('status-chart'));

// Initial load
async function updateDashboard() {
    const response = await fetch('/api/v1/analytics/signal-health/live');
    const data = await response.json();

    statusChart.setOption({
        series: [{ data: data.health_scores }]
    });
}

// Load immediately
updateDashboard();

// Poll every 30 seconds
setInterval(updateDashboard, 30000);
```

**Flow (WebSocket)**:
```
User opens dashboard
    ↓
JavaScript opens WebSocket connection
    ↓
Server pushes updates when data changes
    ↓
JavaScript receives message
    ↓
JavaScript updates chart (chart.setOption(...))
```

**Example**: Real-time event stream

```javascript
const ws = new WebSocket('ws://localhost:8000/ws/events');

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);

    // Append new data point to existing chart
    const currentData = chart.getOption().series[0].data;
    currentData.push(data.new_event);

    // Update chart
    chart.setOption({
        series: [{ data: currentData }]
    });
};
```

---

## Directory Structure

```
tsigma/
├── templates/
│   ├── base.html              # Base layout (header, nav, footer)
│   ├── login.html             # Login page
│   ├── pages/
│   │   ├── dashboard.html     # Main dashboard
│   │   ├── signals/
│   │   │   ├── index.html     # Signal list
│   │   │   └── detail.html    # Signal detail page
│   │   ├── reports/
│   │   │   ├── index.html     # Report selection
│   │   │   └── viewer.html    # Report viewer
│   │   └── admin/
│   │       ├── users.html     # User management
│   │       └── settings.html  # System settings
│   └── components/
│       ├── nav.html
│       ├── sidebar.html
│       └── signal_card.html
├── static/
│   ├── vendor/                # Vendor-downloaded libraries (committed)
│   │   ├── alpine/
│   │   ├── echarts/
│   │   ├── maplibre/
│   │   └── tailwind/
│   ├── js/
│   │   ├── charts/            # 22 report chart modules
│   │   │   ├── pcd.js
│   │   │   ├── approach_delay.js
│   │   │   ├── approach_volume.js
│   │   │   ├── split_monitor.js
│   │   │   └── ...
│   │   ├── common.js          # Shared utilities
│   │   ├── dashboard.js       # Dashboard logic
│   │   └── signals.js         # Signal list logic
│   └── css/
│       └── custom.css         # Minimal custom CSS
└── api/
    └── ui.py                  # UI routes (render templates)
```

---

## Why This Approach?

### Advantages Over SPA

| Aspect | SPA (React/Vue) | TSIGMA | Advantage |
|--------|----------------|--------|-----------|
| **Initial Load** | Slow (download entire app) | **Fast** (HTML only) | ✅ TSIGMA |
| **Build Step** | Required (webpack, vite) | **None** (vendor files) | ✅ TSIGMA |
| **Air-Gapped** | Fails (needs npm install) | **Works** (libs committed) | ✅ TSIGMA |
| **Maintenance** | npm dependencies (CVEs) | **Minimal** (5 vendor files) | ✅ TSIGMA |
| **Learning Curve** | High (React/Vue/TypeScript) | **Low** (HTML + vanilla JS) | ✅ TSIGMA |
| **Deployment** | Build artifacts | **Single binary** | ✅ TSIGMA |
| **Chart Updates** | Often destroy/recreate | **Efficient** (update data only) | ✅ TSIGMA |

### Advantages Over Server-Only Rendering

| Aspect | Full SSR | TSIGMA | Advantage |
|--------|----------|--------|-----------|
| **Interactivity** | Limited (page reloads) | **Rich** (Alpine + charts) | ✅ TSIGMA |
| **Performance** | Slow (full page reload) | **Fast** (JSON API updates) | ✅ TSIGMA |
| **User Experience** | Poor (flash on reload) | **Smooth** (no flash) | ✅ TSIGMA |

---

## Key Principles

### 1. Zero Build Step

No npm, no package.json, no webpack, no bundlers. Download vendor libraries once, commit them, done.

### 2. Air-Gapped Compatible

All dependencies are local files. Works in environments with no internet access.

### 3. Efficient Chart Updates

Initialize charts once, update data in-place. Don't destroy and recreate DOM elements.

```javascript
// ❌ Bad: Destroys and recreates chart on every update
function updateChart(data) {
    const container = document.getElementById('chart');
    container.innerHTML = '';  // Destroys chart
    const newChart = echarts.init(container);  // Recreates chart
    newChart.setOption({ series: [{ data: data }] });
}

// ✅ Good: Updates data only
const chart = echarts.init(document.getElementById('chart'));  // Once

function updateChart(data) {
    chart.setOption({ series: [{ data: data }] });  // Efficient update
}
```

### 4. JSON APIs for Data

FastAPI endpoints return JSON, not HTML. JavaScript fetches JSON and updates charts.

```python
# ✅ Correct: Return JSON
@app.get("/api/v1/analytics/volume")
async def get_volume_data(signal_id: str, start: str, end: str):
    data = await compute_volume(signal_id, start, end)
    return {"bins": data.bins, "volumes": data.volumes}  # JSON
```

### 5. Progressive Enhancement

Basic HTML works without JavaScript. JavaScript enhances with interactivity and charts.

---

## Performance Guidance

| Metric | Goal | Method |
|--------|------|--------|
| **Initial page load** | Fast | Server-rendered HTML minimizes round trips |
| **Chart render** | Fast | ECharts lazy initialization |
| **Data fetch + update** | Responsive | JSON API + efficient chart update |
| **Real-time update** | Near-instant | WebSocket push + chart update |

Actual performance depends on deployment hardware, network conditions, and data volume. Validate with benchmarking after deployment.

---

## Example: PCD Chart Page

### Template (tsigma/templates/analytics/pcd.html)

```html
{% extends "base.html" %}

{% block content %}
<div class="container mx-auto px-4 py-8">
    <h1 class="text-3xl font-bold mb-6">Purdue Coordination Diagram</h1>

    <!-- Controls (Alpine.js state) -->
    <div x-data="{
        signalId: 'gdot-0142',
        phase: 2,
        startDate: '2026-03-03T14:00',
        endDate: '2026-03-03T14:15'
    }">
        <!-- Input fields -->
        <div class="grid grid-cols-4 gap-4 mb-6">
            <input type="text" x-model="signalId" placeholder="Signal ID"
                   class="border rounded px-3 py-2">
            <input type="number" x-model="phase" placeholder="Phase"
                   class="border rounded px-3 py-2">
            <input type="datetime-local" x-model="startDate"
                   class="border rounded px-3 py-2">
            <input type="datetime-local" x-model="endDate"
                   class="border rounded px-3 py-2">
        </div>

        <!-- Run button -->
        <button @click="loadPCDData(signalId, phase, startDate, endDate)"
                class="bg-blue-500 text-white px-6 py-2 rounded mb-6">
            Run Analysis
        </button>

        <!-- Chart container -->
        <div id="pcd-chart" class="bg-white shadow-lg rounded-lg p-6"
             style="width: 100%; height: 600px;">
        </div>
    </div>
</div>

<!-- Chart logic -->
<script src="/static/js/charts/pcd.js"></script>
{% endblock %}
```

### Chart Script (tsigma/static/js/charts/pcd.js)

```javascript
// Initialize chart once
const pcdChart = echarts.init(document.getElementById('pcd-chart'));

// Set initial configuration
pcdChart.setOption({
    title: { text: 'Purdue Coordination Diagram' },
    xAxis: { type: 'time' },
    yAxis: { name: 'Detector' },
    series: [
        { name: 'Arrivals', type: 'scatter', data: [] },
        { name: 'Green Band', type: 'line', data: [], areaStyle: { color: '#4ade80' } }
    ]
});

// Fetch and update data
async function loadPCDData(signalId, phase, start, end) {
    const response = await fetch(`/api/v1/analytics/pcd?` + new URLSearchParams({
        signal_id: signalId,
        phase: phase,
        start: start,
        end: end
    }));

    const data = await response.json();

    // Update chart data (efficient)
    pcdChart.setOption({
        series: [
            { data: data.arrivals },
            { data: data.green_band }
        ]
    });
}

// Resize on window resize
window.addEventListener('resize', () => pcdChart.resize());

// Make function globally available for Alpine.js
window.loadPCDData = loadPCDData;
```

---

## Security Benefits

**No JavaScript Build = No npm Vulnerabilities**:
- ✅ No webpack exploits
- ✅ No transitive dependency CVEs
- ✅ Vendor files use subresource integrity (SRI) hashes (optional)
- ✅ Server controls all logic

**Progressive Enhancement**:
- ✅ Basic functionality works without JavaScript
- ✅ Enhanced with JavaScript (better UX)

---

## Deployment

**Single binary deployment**:
```bash
# All vendor libraries are committed to git
git clone https://github.com/tsigma/tsigma.git
cd tsigma

# No npm install, no build step needed
uvicorn tsigma.api:app --host 0.0.0.0 --port 8000
```

**Docker**:
```dockerfile
FROM python:3.14-slim

WORKDIR /app
COPY . .

RUN pip install -e .

# No npm install, no build step
CMD ["uvicorn", "tsigma.api:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

## Recommendation

**Start with**:
1. Download vendor libraries (one-time setup)
2. Create base template (header, nav, layout)
3. Create dashboard page (signal selector)
4. Create PCD chart page (JSON API + ECharts)

**Timeline**: 1-2 weeks for functional UI with core charts

---

**Document Version**: 2.0
**Last Updated**: 2026-03-05
**Owner**: TSIGMA Development Team
