# Web UI

> Part of [TSIGMA Architecture](../ARCHITECTURE.md)

---

## Technology Stack

| Component | Purpose | Source |
|-----------|---------|--------|
| **Jinja2** | Server-side templating | Python standard |
| **Alpine.js** | Reactive UI components | Vendor downloaded |
| **Vanilla JavaScript** | Data fetching, chart updates | Native browser |
| **ECharts** | Charts and visualizations | Vendor downloaded |
| **MapLibre GL JS** | Interactive maps | Vendor downloaded |
| **Tailwind CSS** | Utility-first styling | Vendor downloaded |
| **WebSocket** | Real-time updates (optional) | Native browser API |

## Key Principles

- **Zero build step** - No npm, no webpack, no bundlers
- **Air-gapped compatible** - All libraries committed to repo
- **Efficient chart updates** - Update data without destroying charts
- **JSON APIs** - All data fetching uses REST APIs returning JSON

## Template Structure

```
templates/
├── base.html              # Main layout
├── login.html             # Login page (public)
├── pages/
│   ├── dashboard.html     # Main dashboard
│   ├── signals/
│   │   ├── index.html     # Signal list
│   │   └── detail.html    # Signal detail
│   ├── reports/
│   │   ├── index.html     # Report selection
│   │   └── viewer.html    # Report viewer
│   └── admin/
│       ├── users.html     # User management
│       └── settings.html  # System settings
└── components/
    ├── nav.html
    ├── sidebar.html
    └── signal_card.html
```

## Vendor Libraries Setup

All vendor libraries are downloaded once and committed to the repository.

**Directory structure**:
```
tsigma/static/vendor/
├── alpine/
│   └── alpine.min.js           # Alpine.js 3.13.5
├── echarts/
│   └── echarts.min.js          # ECharts 5.4.3
├── maplibre/
│   ├── maplibre-gl.js          # MapLibre GL JS 3.6.2
│   └── maplibre-gl.css
└── tailwind/
    └── tailwind.min.js         # Tailwind CSS CDN build
```

**Download script** (run once):
```bash
#!/bin/bash
# scripts/download_vendor_libs.sh

VENDOR_DIR="tsigma/static/vendor"

curl -o $VENDOR_DIR/alpine/alpine.min.js \
  https://cdn.jsdelivr.net/npm/alpinejs@3.13.5/dist/cdn.min.js

curl -o $VENDOR_DIR/echarts/echarts.min.js \
  https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js

curl -o $VENDOR_DIR/maplibre/maplibre-gl.js \
  https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.js

curl -o $VENDOR_DIR/maplibre/maplibre-gl.css \
  https://unpkg.com/maplibre-gl@3.6.2/dist/maplibre-gl.css

curl -o $VENDOR_DIR/tailwind/tailwind.min.js \
  https://cdn.tailwindcss.com/3.4.1
```

## Example: JSON API + Chart Update

```html
<!-- Signal list -->
<div class="grid grid-cols-3 gap-4">
    {% for signal in signals %}
    <div class="bg-white rounded-lg shadow p-4">
        <h3 class="text-lg font-semibold">{{ signal.name }}</h3>
        <p class="text-gray-600">{{ signal.signal_id }}</p>

        <!-- Status loaded via JSON API -->
        <div id="status-{{ signal.signal_id }}" class="mt-2">
            <span class="text-gray-400">Loading...</span>
        </div>
    </div>
    {% endfor %}
</div>

<script>
// Fetch status for each signal via JSON API
async function loadSignalStatus(signalId) {
    const response = await fetch(`/api/v1/signals/${signalId}/status`);
    const data = await response.json();

    // Update DOM with status
    const el = document.getElementById(`status-${signalId}`);
    el.textContent = '';
    const span = document.createElement('span');
    span.className = 'text-sm font-medium';
    span.style.color = data.color;
    span.textContent = data.status;
    el.appendChild(span);
}

// Load all statuses
{% for signal in signals %}
loadSignalStatus('{{ signal.signal_id }}');
{% endfor %}
</script>
```

## Example: Alpine.js Reactivity

```html
<!-- Filter panel with Alpine.js -->
<div x-data="{ open: false, filters: { jurisdiction: '', search: '' } }">
    <button
        @click="open = !open"
        class="bg-blue-500 text-white px-4 py-2 rounded">
        Filters
    </button>

    <div x-show="open" class="mt-2 p-4 bg-white rounded shadow">
        <input
            type="text"
            x-model="filters.search"
            placeholder="Search..."
            class="border rounded px-3 py-2 w-full">

        <select x-model="filters.jurisdiction" class="mt-2 border rounded px-3 py-2 w-full">
            <option value="">All Jurisdictions</option>
            {% for j in jurisdictions %}
            <option value="{{ j.id }}">{{ j.name }}</option>
            {% endfor %}
        </select>

        <button
            @click="applyFilters(filters)"
            class="mt-2 bg-green-500 text-white px-4 py-2 rounded">
            Apply
        </button>
    </div>
</div>

<script>
async function applyFilters(filters) {
    const response = await fetch('/api/v1/signals?' + new URLSearchParams({
        search: filters.search,
        jurisdiction: filters.jurisdiction
    }));

    const signals = await response.json();

    // Update signal list (vanilla JS)
    updateSignalList(signals);
}
</script>
```

## Example: ECharts Integration

```html
<!-- Time-series chart -->
<div id="delay-chart" class="h-96 w-full"></div>

<script>
    // Initialize chart once
    const chart = echarts.init(document.getElementById('delay-chart'));

    // Set initial configuration
    chart.setOption({
        title: { text: 'Approach Delay' },
        xAxis: { type: 'time' },
        yAxis: { type: 'value', name: 'Delay (sec)' },
        dataZoom: [
            { type: 'inside' },
            { type: 'slider' }
        ],
        series: [{
            type: 'line',
            data: []  // Empty initially
        }]
    });

    // Fetch data from JSON API
    async function loadDelayData(signalId, start, end) {
        const response = await fetch(`/api/v1/analytics/delay?` + new URLSearchParams({
            signal_id: signalId,
            start: start,
            end: end
        }));

        const data = await response.json();

        // Efficient update (doesn't recreate chart)
        chart.setOption({
            series: [{ data: data.points }]
        });
    }

    // Resize on window resize
    window.addEventListener('resize', () => chart.resize());

    // Load initial data
    loadDelayData('default-id', '2026-03-01', '2026-03-05');
</script>
```

## Example: MapLibre Integration

```html
<!-- Signal locations map -->
<div id="map" class="h-96 w-full rounded-lg"></div>

<script>
    const map = new maplibregl.Map({
        container: 'map',
        style: '/static/map/style.json',  // Self-hosted style
        center: [-84.388, 33.749],
        zoom: 10
    });

    map.on('load', async () => {
        // Fetch signal locations from JSON API
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

## UI Patterns

### Historical Analysis (User-Triggered)

User selects parameters, clicks "Run Analysis", sees results.

```javascript
document.getElementById('run-analysis').addEventListener('click', async () => {
    const signalId = document.getElementById('signal-select').value;
    const startDate = document.getElementById('start-date').value;
    const endDate = document.getElementById('end-date').value;

    // Fetch data from JSON API
    const response = await fetch(`/api/v1/analytics/volume?` + new URLSearchParams({
        signal_id: signalId,
        start: startDate,
        end: endDate
    }));

    const data = await response.json();

    // Update chart
    volumeChart.setOption({
        xAxis: { data: data.bins },
        series: [{ data: data.volumes }]
    });
});
```

### Live Dashboard (Polling)

Dashboard shows real-time data, auto-updates every 30 seconds.

```javascript
// Initialize chart
const statusChart = echarts.init(document.getElementById('status-chart'));

// Update function
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

### Live Dashboard (WebSocket)

Real-time updates via WebSocket for sub-second latency.

```javascript
const ws = new WebSocket('ws://localhost:8000/ws/events');

ws.onmessage = (event) => {
    const data = JSON.parse(event.data);

    // Append new data point
    const currentData = chart.getOption().series[0].data;
    currentData.push(data.new_event);

    // Update chart
    chart.setOption({
        series: [{ data: currentData }]
    });
};
```

## Key Differences from Traditional Approaches

### vs. SPA (React/Vue)

| Aspect | SPA | TSIGMA |
|--------|-----|--------|
| Build step | Required | None |
| Initial load | Slow | Fast |
| Air-gapped | Fails | Works |
| Dependencies | npm (hundreds) | Vendor files (5) |

### vs. Server-Only

| Aspect | Server-Only | TSIGMA |
|--------|-------------|--------|
| Interactivity | Limited | Rich |
| Page reloads | Every action | Only navigation |
| Chart updates | Full page reload | Efficient data update |

## Deployment

No build step required. All vendor libraries are committed to the repository.

```bash
# Clone and run
git clone https://github.com/tsigma/tsigma.git
cd tsigma
uvicorn tsigma.api:app --host 0.0.0.0 --port 8000
```

Docker deployment:

```dockerfile
FROM python:3.14-slim

WORKDIR /app
COPY . .

RUN pip install -e .

# No npm install, no build step needed
CMD ["uvicorn", "tsigma.api:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

**See Also**: [UI_ARCHITECTURE.md](UI_ARCHITECTURE.md) for detailed architecture and examples.
