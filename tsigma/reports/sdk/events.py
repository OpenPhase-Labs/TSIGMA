"""
Indiana Hi-Resolution Data Logger event code constants and groupings.

Authoritative source: Sturdevant et al., "Indiana Traffic Signal Hi
Resolution Data Logger Enumerations," INDOT/Purdue, November 2012
(doi:10.4231/K4RN35SH).  Cross-checked against UDOT ATSPM 5.x
IndianaEnumerations.cs and ATSPM 4x MOE.Common usages.

Every code used by any TSIGMA report plugin lives here.  Do not
re-define these locally in report files.
"""

# Phase state / termination
EVENT_PHASE_GREEN = 1
EVENT_GAP_OUT = 4
EVENT_MAX_OUT = 5
EVENT_FORCE_OFF = 6
EVENT_GREEN_TERMINATION = 7
EVENT_YELLOW_CLEARANCE = 8
EVENT_RED_CLEARANCE = 9
EVENT_PHASE_END = 10

# Pedestrian
EVENT_PED_WALK = 21
EVENT_PED_CLEARANCE = 22
EVENT_PED_DONT_WALK = 23
EVENT_PED_CALL = 45

# Detectors
EVENT_DETECTOR_OFF = 81
EVENT_DETECTOR_ON = 82

# Preemption (per Indiana Hi-Res spec, Purdue/INDOT 2012, codes 101-130)
EVENT_PREEMPTION_ADVANCE_WARNING = 101         # PreemptAdvanceWarningInput
EVENT_PREEMPTION_CALL_INPUT_ON = 102           # PreemptCallInputOn
EVENT_PREEMPTION_GATE_DOWN = 103               # PreemptGateDownInputReceived
EVENT_PREEMPTION_CALL_INPUT_OFF = 104          # PreemptCallInputOff
EVENT_PREEMPTION_ENTRY_STARTED = 105           # PreemptEntryStarted
EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE = 106   # PreemptionBeginTrackClearance
EVENT_PREEMPTION_BEGIN_DWELL = 107             # PreemptionBeginDwellService
EVENT_PREEMPTION_LINK_ACTIVE_ON = 108          # PreemptionLinkActiveOn
EVENT_PREEMPTION_LINK_ACTIVE_OFF = 109         # PreemptionLinkActiveOff
EVENT_PREEMPTION_MAX_PRESENCE = 110            # PreemptionMaxPresenceExceeded
EVENT_PREEMPTION_BEGIN_EXIT = 111              # PreemptionBeginExitInterval

# Transit signal priority (per Indiana Hi-Res spec, codes 112-115)
EVENT_TSP_CHECK_IN = 112               # TSPCheckIn — priority request received
EVENT_TSP_EARLY_GREEN = 113            # TSPAdjustmenttoEarlyGreen
EVENT_TSP_EXTEND_GREEN = 114           # TSPAdjustmenttoExtendGreen
EVENT_TSP_CHECK_OUT = 115              # TSPCheckOut — priority request retracted

# Groupings
DETECTOR_EVENT_CODES: tuple[int, ...] = (EVENT_DETECTOR_OFF, EVENT_DETECTOR_ON)

TERMINATION_CODES: dict[int, str] = {
    EVENT_GAP_OUT: "gap_out",
    EVENT_MAX_OUT: "max_out",
    EVENT_FORCE_OFF: "force_off",
}
TERMINATION_NAMES: tuple[str, ...] = ("gap_out", "max_out", "force_off")

# Human-readable names for all standard event codes.
EVENT_NAMES: dict[int, str] = {
    EVENT_PHASE_GREEN: "Phase Green",
    EVENT_GAP_OUT: "Gap Out",
    EVENT_MAX_OUT: "Max Out",
    EVENT_FORCE_OFF: "Force Off",
    EVENT_GREEN_TERMINATION: "Green Termination",
    EVENT_YELLOW_CLEARANCE: "Yellow Clearance",
    EVENT_RED_CLEARANCE: "Red Clearance",
    EVENT_PHASE_END: "Phase End",
    EVENT_PED_WALK: "Pedestrian Walk",
    EVENT_PED_CLEARANCE: "Pedestrian Clearance",
    EVENT_PED_DONT_WALK: "Pedestrian Don't Walk",
    EVENT_PED_CALL: "Pedestrian Call",
    EVENT_DETECTOR_OFF: "Detector Off",
    EVENT_DETECTOR_ON: "Detector On",
    EVENT_PREEMPTION_ADVANCE_WARNING: "Preempt Advance Warning",
    EVENT_PREEMPTION_CALL_INPUT_ON: "Preempt Call Input On",
    EVENT_PREEMPTION_GATE_DOWN: "Preempt Gate Down Input",
    EVENT_PREEMPTION_CALL_INPUT_OFF: "Preempt Call Input Off",
    EVENT_PREEMPTION_ENTRY_STARTED: "Preempt Entry Started",
    EVENT_PREEMPTION_BEGIN_TRACK_CLEARANCE: "Preemption Begin Track Clearance",
    EVENT_PREEMPTION_BEGIN_DWELL: "Preemption Begin Dwell Service",
    EVENT_PREEMPTION_LINK_ACTIVE_ON: "Preemption Link Active On",
    EVENT_PREEMPTION_LINK_ACTIVE_OFF: "Preemption Link Active Off",
    EVENT_PREEMPTION_MAX_PRESENCE: "Preemption Max Presence Exceeded",
    EVENT_PREEMPTION_BEGIN_EXIT: "Preemption Begin Exit Interval",
    EVENT_TSP_CHECK_IN: "TSP Check In",
    EVENT_TSP_EARLY_GREEN: "TSP Adjustment to Early Green",
    EVENT_TSP_EXTEND_GREEN: "TSP Adjustment to Extend Green",
    EVENT_TSP_CHECK_OUT: "TSP Check Out",
}

# Approach direction_type_id -> compass abbreviation.
DIRECTION_MAP: dict[int, str] = {1: "NB", 2: "SB", 3: "EB", 4: "WB"}
