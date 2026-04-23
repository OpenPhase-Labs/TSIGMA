# Indiana HiRes Event Code Reference

This document lists the high-resolution event codes that traffic signal controllers emit to their data loggers. The codes follow the **Indiana Traffic Signal Hi-Resolution Data Logger Enumerations** (Purdue/INDOT, November 2012, doi:10.4231/K4RN35SH) — the de-facto standard used by every ATSPM-compatible controller and consumed by TSIGMA.

This is a related-but-distinct standard from NTCIP 1202 v3. Where both standards define the same concept, Indiana HiRes is what controllers actually log.

## Event Code Format

Each event record contains:
- **Timestamp**: When the event occurred (0.1-second resolution)
- **Event Code**: Type of event (0–255)
- **Event Parameter**: Context-specific value (typically a phase/detector/preempt/overlap number)

---

## Active Phase Events (0–20)

Phase state transitions for each NEMA movement.

| Code | Name | Parameter | Description |
|------|------|-----------|-------------|
| 0 | Phase On | Phase # (1–16) | NEMA Phase On becomes active (start of green or walk, whichever is first) |
| 1 | Phase Begin Green | Phase # (1–16) | Solid or flashing green begins (not repeated during flashing) |
| 2 | Phase Check | Phase # (1–16) | Conflicting call registered against active phase (begins MAX timing) |
| 3 | Phase Min Complete | Phase # (1–16) | Phase minimum-green timer expired |
| 4 | Phase Gap Out | Phase # (1–16) | Phase gapped out (may be set multiple times under simultaneous gap-out) |
| 5 | Phase Max Out | Phase # (1–16) | Phase MAX timer expired |
| 6 | Phase Force Off | Phase # (1–16) | Force-off applied to active green phase |
| 7 | Phase Green Termination | Phase # (1–16) | Green terminated into yellow or permissive (FYA) |
| 8 | Phase Begin Yellow Clearance | Phase # (1–16) | Yellow indication active, clearance timer begins |
| 9 | Phase End Yellow Clearance | Phase # (1–16) | Yellow indication ends |
| 10 | Phase Begin Red Clearance | Phase # (1–16) | Red clearance timing begins (only set if red clearance is served) |
| 11 | Phase End Red Clearance | Phase # (1–16) | Red clearance timing concludes |
| 12 | Phase Inactive | Phase # (1–16) | Phase no longer active in ring |
| 13–20 | *Reserved* | — | Reserved for future use |

### Cycle Detection

**Red-to-Red Cycle**: Most ATSPM analyses define a cycle from one Phase Begin Red Clearance (10) to the next for the same phase.

**Green-to-Green Cycle**: Alternative measurement using Phase Begin Green (1) — common for coordinated phases.

---

## Active Pedestrian Events (21–30)

| Code | Name | Parameter | Description |
|------|------|-----------|-------------|
| 21 | Pedestrian Begin Walk | Phase # (1–16) | Walk indication active |
| 22 | Pedestrian Begin Clearance | Phase # (1–16) | Flashing Don't Walk (FDW) active |
| 23 | Pedestrian Begin Solid Don't Walk | Phase # (1–16) | Don't Walk solid (after FDW or after dark interval) |
| 24 | Pedestrian Dark | Phase # (1–16) | Pedestrian outputs off |
| 25–30 | *Reserved* | — | Reserved for future use |

### Pedestrian Delay

Measured from **Pedestrian Call Registered (45)** to **Pedestrian Begin Walk (21)**.

---

## Barrier / Ring Events (31–40)

| Code | Name | Parameter | Description |
|------|------|-----------|-------------|
| 31 | Barrier Termination | Barrier # (1–8) | All active phases inactive in ring; cross-barrier phases next |
| 32 | FYA – Begin Permissive | FYA # (1–4) | Flashing yellow arrow active |
| 33 | FYA – End Permissive | FYA # (1–4) | FYA inactive (cleared or transitioned to protected) |
| 34–40 | *Reserved* | — | Reserved for future use |

---

## Phase Control Events (41–60)

| Code | Name | Parameter | Description |
|------|------|-----------|-------------|
| 41 | Phase Hold Active | Phase # (1–16) | Hold applied by coordinator/preemptor/external logic |
| 42 | Phase Hold Released | Phase # (1–16) | Hold released |
| 43 | Phase Call Registered | Phase # (1–16) | Vehicular call registered (not set if recall exists) |
| 44 | Phase Call Dropped | Phase # (1–16) | Call cleared (by service or removal) |
| 45 | Pedestrian Call Registered | Phase # (1–16) | Pedestrian call registered (not set if recall exists) |
| 46 | Phase Omit On | Phase # (1–16) | Phase omit applied (dynamic, not configuration) |
| 47 | Phase Omit Off | Phase # (1–16) | Phase omit released |
| 48 | Pedestrian Omit On | Phase # (1–16) | Ped omit applied |
| 49 | Pedestrian Omit Off | Phase # (1–16) | Ped omit released |
| 50–60 | *Reserved* | — | Reserved for future use |

---

## Overlap Events (61–80)

Overlap numbers are encoded as integers (A=1, B=2, C=3, …).

| Code | Name | Parameter | Description |
|------|------|-----------|-------------|
| 61 | Overlap Begin Green | Overlap # | Overlap green active (not repeated during flashing) |
| 62 | Overlap Begin Trailing Green (Extension) | Overlap # | Overlap green extension timing begins |
| 63 | Overlap Begin Yellow | Overlap # | Overlap yellow clearance |
| 64 | Overlap Begin Red Clearance | Overlap # | Overlap red clearance timing begins |
| 65 | Overlap Off (Inactive) | Overlap # | Overlap completed all timing; conflicting phases may begin |
| 66 | Overlap Dark | Overlap # | Overlap head set dark (no active outputs) |
| 67 | Pedestrian Overlap Begin Walk | Overlap # | Walk indication active |
| 68 | Pedestrian Overlap Begin Clearance | Overlap # | FDW active |
| 69 | Pedestrian Overlap Begin Solid Don't Walk | Overlap # | Don't Walk solid |
| 70 | Pedestrian Overlap Dark | Overlap # | Pedestrian overlap outputs off |
| 71–80 | *Reserved* | — | Reserved for future use |

---

## Detector Events (81–100)

Vehicle and pedestrian detector activations and faults. **Detector on/off events are post detector delay/extension processing.**

| Code | Name | Parameter | Description |
|------|------|-----------|-------------|
| 81 | Detector Off | Det Channel # (1–64) | Vehicle left detection zone |
| 82 | Detector On | Det Channel # (1–64) | Vehicle entered detection zone |
| 83 | Detector Restored | Det Channel # (1–64) | Detector restored to non-failed state |
| 84 | Detector Fault — Other | Det Channel # (1–64) | Local controller diagnostic failure |
| 85 | Detector Fault — Watchdog | Det Channel # (1–64) | Watchdog fault |
| 86 | Detector Fault — Open Loop | Det Channel # (1–64) | Open loop fault |
| 87 | Detector Fault — Shorted Loop | Det Channel # (1–64) | Shorted loop fault |
| 88 | Detector Fault — Excessive Change | Det Channel # (1–64) | Excessive change fault |
| 89 | Pedestrian Detector Off | Det Channel # (1–16) | Ped detector inactive |
| 90 | Pedestrian Detector On | Det Channel # (1–16) | Ped detector active (may set multiple times per call) |
| 91 | Pedestrian Detector Failed | Ped Det # (1–16) | Local controller diagnostic failure |
| 92 | Pedestrian Detector Restored | Ped Det # (1–16) | Restored to non-failed state |
| 93–100 | *Reserved* | — | Reserved for future use |

### Detector Analytics

**Stuck Detection**: A detector continuously ON or OFF for an extended period (commonly > 30 minutes) suggests malfunction.

**Occupancy**: Percentage of analysis period the detector is ON:
```
Occupancy = (Total ON Time / Analysis Period) × 100
```

**Gap Analysis**: Time between successive activations. Short gaps indicate congestion.

---

## Preemption Events (101–130)

Emergency-vehicle and railroad preemption, plus transit signal priority (TSP).

| Code | Name | Parameter | Description |
|------|------|-----------|-------------|
| 101 | Preempt Advance Warning Input | Preempt # (1–10) | Advance warning input activated |
| 102 | Preempt (Call) Input On | Preempt # (1–10) | Preemption input activated (prior to delay timing); may repeat if intermittent |
| 103 | Preempt Gate Down Input Received | Preempt # (1–10) | Railroad gate-down input received (if available) |
| 104 | Preempt (Call) Input Off | Preempt # (1–10) | Preemption input deactivated; may repeat if intermittent |
| 105 | Preempt Entry Started | Preempt # (1–10) | Preemption delay expired; controller begins transition (force off) to serve preemption |
| 106 | Preemption Begin Track Clearance | Preempt # (1–10) | Track clearance phases green; track clearance timing begins |
| 107 | Preemption Begin Dwell Service | Preempt # (1–10) | Dwell or limited service begins (or min-dwell timer reset) |
| 108 | Preemption Link Active On | Preempt # (1–10) | Linked preemptor input applied from active preemptor |
| 109 | Preemption Link Active Off | Preempt # (1–10) | Linked preemptor input dropped |
| 110 | Preemption Max Presence Exceeded | Preempt # (1–10) | Max-presence timer exceeded; input released |
| 111 | Preemption Begin Exit Interval | Preempt # (1–10) | Exit-interval phases green; exit timing begins |
| 112 | TSP Check In | TSP # (1–10) | Priority request received |
| 113 | TSP Adjustment to Early Green | TSP # (1–10) | Cycle adjusted for early service to TSP phases |
| 114 | TSP Adjustment to Extend Green | TSP # (1–10) | Cycle adjusted for extended service to TSP phases |
| 115 | TSP Check Out | TSP # (1–10) | Priority request retracted |
| 116–130 | *Reserved* | — | Reserved for future use |

### Common Preemption Pairings

| Metric | Begin → End |
|--------|-------------|
| **Preempt request duration** (input active) | 102 → 104 |
| **Preempt service duration** (controller serving preemption) | 105 → 111 |
| **TSP request lifetime** | 112 → 115 |

> Some vendors (notably extensions in UDOT ATSPM 5.x) define additional codes 116–119 for `Preemption Force Off Phase`, `TSP Early Force Off Cycle`, `TSP Service Start`, and `TSP Service End`. These are vendor extensions, not part of the 2012 baseline spec.

---

## Coordination Events (131–170)

Coordination plan, cycle, offset, and split events.

| Code | Name | Parameter | Description |
|------|------|-----------|-------------|
| 131 | Coord Pattern Change | Pattern # (0–255) | Active coordination pattern (highest priority of TOD/system/manual). Not reapplied during preemption suspension. |
| 132 | Cycle Length Change | Seconds (0–255) | Cycle length on new pattern; > 255 sec recorded as 255 (DB lookup needed) |
| 133 | Offset Length Change | Seconds (0–255) | Offset on new pattern; > 255 sec recorded as 255 |
| 134 | Split 1 Change | Seconds (0–255) | New split time for phase 1 |
| 135 | Split 2 Change | Seconds (0–255) | New split time for phase 2 |
| 136 | Split 3 Change | Seconds (0–255) | New split time for phase 3 |
| 137 | Split 4 Change | Seconds (0–255) | New split time for phase 4 |
| 138 | Split 5 Change | Seconds (0–255) | New split time for phase 5 |
| 139 | Split 6 Change | Seconds (0–255) | New split time for phase 6 |
| 140 | Split 7 Change | Seconds (0–255) | New split time for phase 7 |
| 141 | Split 8 Change | Seconds (0–255) | New split time for phase 8 |
| 142 | Split 9 Change | Seconds (0–255) | New split time for phase 9 |
| 143 | Split 10 Change | Seconds (0–255) | New split time for phase 10 |
| 144 | Split 11 Change | Seconds (0–255) | New split time for phase 11 |
| 145 | Split 12 Change | Seconds (0–255) | New split time for phase 12 |
| 146 | Split 13 Change | Seconds (0–255) | New split time for phase 13 |
| 147 | Split 14 Change | Seconds (0–255) | New split time for phase 14 |
| 148 | Split 15 Change | Seconds (0–255) | New split time for phase 15 |
| 149 | Split 16 Change | Seconds (0–255) | New split time for phase 16 |
| 150 | Coord Cycle State Change | State (0–6) | See state values below |
| 151 | Coordinated Phase Yield Point | Phase # (1–16) | Coordinated phase yield/cycle reference point |
| 152–170 | *Reserved* | — | Reserved for future use |

### Event 150 — Coord Cycle State Values

| Value | State |
|-------|-------|
| 0 | Free |
| 1 | In Step |
| 2 | Transition — Add |
| 3 | Transition — Subtract |
| 4 | Transition — Dwell |
| 5 | Local Zero |
| 6 | Begin Pickup |

### Coordination Metrics

**Offset Drift**: Standard deviation of cycle length sampled at the coordinated phase yield point (151).

**Coordination Quality**: Percentage of cycles where offset error is within tolerance (typically ±2 seconds).

---

## Cabinet / System Events (171–199)

Controller cabinet inputs/outputs and system status.

| Code | Name | Parameter | Description |
|------|------|-----------|-------------|
| 171 | Test Input On | Test Input # | Cabinet test or special-function input |
| 172 | Test Input Off | Test Input # | Cabinet test/special-function input released |
| 173 | Unit Flash Status Change | NTCIP Flash State (0–255) | See NTCIP 1202 §2.4.5 |
| 174 | Unit Alarm Status 1 Change | NTCIP Alarm Status 1 (0–255) | See NTCIP 1202 §2.4.8 |
| 175 | Alarm Group State Change | NTCIP Alarm Group State (0–255) | See NTCIP 1202 §2.4.12.2 |
| 176 | Special Function Output On | Special Function # (0–255) | Vendor-defined |
| 177 | Special Function Output Off | Special Function # (0–255) | Vendor-defined |
| 178 | Manual Control Enable Off/On | 0 or 1 | Manual control toggled |
| 179 | Interval Advance Off/On | 0 or 1 | Leading edge=1, lagging edge=0 (optional) |
| 180 | Stop Time Input Off/On | 0 or 1 | Stop time input applied/removed |
| 181 | Controller Clock Updated | Time correction (s) | OS clock adjusted via comms/command/input |
| 182 | Power Failure Detected | True (1) | Line voltage 0–89 VAC for > 100 ms |
| 184 | Power Restored | True (1) | Line voltage > 98 VAC restored |
| 185 | Vendor-Specific Alarm | Vendor-defined | Generic placeholder for vendor failure/alarm types |
| 186–199 | *Reserved* | — | Reserved for future use |
| 200–255 | *Reserved* | — | Reserved for future use |

---

## Metric Calculation Reference

### Split Failure Detection

A split failure occurs when demand exceeds capacity during a phase:

1. **Green Occupancy** ≥ 79% at green start (queue present at the stop bar)
2. **Red Occupancy** ≥ 79% in first 5 seconds of red (vehicles couldn't clear)

See `tsigma/reports/split_failure.py` for the implementation.

### Arrivals on Green (AOG)

Classification of vehicle arrivals by signal state:

| Arrival Type | Signal State at Detector ON |
|--------------|------------------------------|
| Green | Phase Begin Green (1) active |
| Yellow | Phase Begin Yellow Clearance (8) active |
| Red | Phase Begin Red Clearance (10) active |

```
Percent AOG = (Arrivals on Green / Total Arrivals) × 100
```

### Volume Factors

| Factor | Formula | Description |
|--------|---------|-------------|
| **PHF** | Peak Hour Volume / (4 × Peak 15-min Volume) | Peak Hour Factor |
| **K-Factor** | Peak Hour Volume / Daily Volume | Design hour ratio |
| **D-Factor** | Directional Volume / Total Volume | Directional split |

---

## References

- **Authoritative spec**: Sturdevant, J. R., et al. *Indiana Traffic Signal Hi Resolution Data Logger Enumerations*. INDOT and Purdue University, November 2012. doi:[10.4231/K4RN35SH](https://doi.org/10.4231/K4RN35SH)
- **Reference implementation**: UDOT ATSPM 5.x — `ATSPM/Data/Enums/IndianaEnumerations.cs`
- **Background**: Smaglik, E. J., et al. "Event-Based Data Collection for Generating Actuated Controller Performance Measures." *TRR* #2035 (2007). doi:[10.3141/2035-11](https://doi.org/10.3141/2035-11)
- **Related**: NTCIP 1202 v3 — Object Definitions for Actuated Traffic Signal Controller Units
