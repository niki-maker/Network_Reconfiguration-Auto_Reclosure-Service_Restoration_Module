import opendssdirect as dss
import pandas as pd
import logging
import os
import time
import threading
import requests
import json
from flask import Flask
from datetime import datetime
from collections import deque
import math
import re
import numpy as np

# ================= Logging Setup =================
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
logger.addHandler(logging.FileHandler("main.log"))
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

EXCEL_PATH = "/shared_volume/grid_data1.xlsx"
ACTIVATED_DEVICES_PATH = "/shared_volume/activated_devices.json"
CB_JSON_PATH = "/shared_volume/cb_states.json"
VOLTAGE_TOL_PU = (0.95, 1.05)

# ================= Circuit Rebuild =================
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2

# ---------------- Helper Functions (from test2.py adapted) ----------------
def map_cb_to_branch_line(cb_protected_line, branch_df):
    """Map CB-style names (Line.L_CB#) to actual branch line names (Line.L#)."""
    if not cb_protected_line:
        return None
    m = re.search(r"Line\.L_CB(\d+)", cb_protected_line)
    if m:
        idx = int(m.group(1))  # CB number
        # Make sure index is in range of branch_df
        if 0 < idx <= len(branch_df):
            return f"Line.L{idx}"  # Correct branch line
        else:
            logging.warning(f"CB number {idx} out of range for branch_df")
            return None
    # Already valid format
    return cb_protected_line

def mag_from_complex_list(cplx_list):
    mags = []
    for i in range(0, len(cplx_list), 2):
        mags.append(math.hypot(cplx_list[i], cplx_list[i + 1]))
    return mags


def complex_list_to_phasors(cplx_list):
    ph = []
    for i in range(0, len(cplx_list), 2):
        re_ = cplx_list[i]
        im = cplx_list[i+1] if i+1 < len(cplx_list) else 0.0
        ph.append(complex(re_, im))
    return ph


def log_bus_voltages(title="Bus Voltages"):
    logging.info(f"\n=== {title.upper()} ===")
    logging.info(f"{'Bus':<20} {'Voltage (p.u.)':>15} {'Angle (deg)':>15}")
    logging.info("-" * 60)
    for bus in dss.Circuit.AllBusNames():
        try:
            dss.Circuit.SetActiveBus(bus)
            volts = dss.Bus.Voltages()
            if not volts:
                continue
            ph = complex_list_to_phasors(volts)
            kv_base = dss.Bus.kVBase()
            if kv_base == 0:
                continue
            avg_v = sum(abs(v) for v in ph) / len(ph)
            pu_voltage = avg_v / (kv_base * 1000)
            angle_deg = np.angle(ph[0], deg=True)
            logging.info(f"{bus:<20} {abs(pu_voltage):>15.4f} {angle_deg:>15.2f}")
        except Exception as e:
            logging.warning(f"Could not log bus {bus}: {e}")


def build_topology_from_feeder_list(feeder_lines):
    adj = {}
    line_to_buses = {}
    for idx, (b1, b2, _, _) in enumerate(feeder_lines, start=1):
        l_name = f"Line.L{idx}"
        bus1 = f"bus{b1}"
        bus2 = f"bus{b2}"
        line_to_buses[l_name] = (bus1, bus2)
        adj.setdefault(bus1, []).append((bus2, l_name))
    return adj, line_to_buses


def bfs_distance(adj, start_bus):
    dist = {start_bus: 0}
    q = deque([start_bus])
    while q:
        b = q.popleft()
        for (nb, _) in adj.get(b, []):
            if nb not in dist:
                dist[nb] = dist[b] + 1
                q.append(nb)
    return dist


def setup_directional_relays(breaker_map, prefault_currents, feeder_lines,
                             pickup_mult=3.0, min_pickup_A=250.0,
                             min_delay=0.05, max_delay=0.6):
    relays = {}
    adj, line_to_buses = build_topology_from_feeder_list(feeder_lines)
    dist_map = bfs_distance(adj, 'bus1')
    dists = []
    for cb, info in breaker_map.items():
        prot = info.get('protected_line')
        if not prot:
            continue
        idxm = re.search(r'\d+', prot)
        if idxm:
            idx = int(idxm.group(0))
            _, b2 = line_to_buses.get(f'Line.L{idx}', (None, None))
            dists.append(dist_map.get(b2, 0))
    max_dist = max(dists) if dists else 1

    for cb_name, info in breaker_map.items():
        protected_line = info.get('protected_line')
        upstream_bus = info.get('upstream_bus')
        preI = 0.0
        if protected_line and protected_line in prefault_currents:
            preI = prefault_currents.get(protected_line, 0.0)
        pickup = max(preI * pickup_mult, min_pickup_A)
        dist = 0
        if protected_line:
            m = re.search(r'\d+', protected_line)
            if m:
                idx = int(m.group(0))
                downstream_bus = build_topology_from_feeder_list(feeder_lines)[1].get(f'Line.L{idx}', (None, None))[1]
                dist = bfs_distance(adj, 'bus1').get(downstream_bus, 0)
        if max_dist <= 0:
            delay = min_delay
        else:
            frac = dist / max_dist
            delay = max_delay - frac * (max_delay - min_delay)
        relays[cb_name] = {
            'upstream_bus': upstream_bus,
            'protected_line': protected_line,
            'pickup_A': pickup,
            'delay_s': delay,
            'operate_time': 0.0,
            'tripped': False
        }
    return relays


def measure_line_currents_phasors(line_elem_name, excel_path=EXCEL_PATH):
    """
    Reads line current magnitudes from Excel and returns approximate 3-phase phasors.
    Suitable for balanced 3-phase faults where current angles are not critical.
    Automatically detects sheet name among common variants.
    """
    import pandas as pd
    import numpy as np
    import re
    import logging
    import time

    # Ensure Excel file exists before reading
    if not os.path.exists(excel_path):
        logging.warning(f"measure_line_currents_phasors: Excel file not found at {excel_path}")
        return []

    # Extract line number from name (e.g., "Line.L28" -> 27)
    match = re.search(r"Line\.L(\d+)", line_elem_name, re.IGNORECASE)
    if not match:
        logging.warning(f"measure_line_currents_phasors: Invalid line element name {line_elem_name}")
        return []

    line_idx = int(match.group(1)) - 1  # zero-based for Excel indexing

    # Retry loop in case Excel file is locked or being updated
    for attempt in range(3):
        try:
            xls = load_excel_with_retry(excel_path)
            sheet_candidates = [s.lower() for s in xls.sheet_names]
            # Try to detect the correct sheet automatically
            if "branches" in sheet_candidates:
                sheet_name = "branches"
            else:
                logging.warning(f"measure_line_currents_phasors: No recognized sheet found in {excel_path}")
                return []

            branch_df = pd.read_excel(xls, sheet_name=sheet_name, engine="openpyxl")

            if line_idx < 0 or line_idx >= len(branch_df):
                logging.warning(f"measure_line_currents_phasors: Line index {line_idx+1} out of range.")
                return []

            # Look for any reasonable column name for current
            current_col = None
            for c in branch_df.columns:
                cl = c.lower()
                if any(key in cl for key in ["line_current_a", "current_a", "current", "i_mag", "i(a)"]):
                    current_col = c
                    break

            if not current_col:
                logging.warning(f"measure_line_currents_phasors: No current column found in {sheet_name}.")
                return []

            I_mag = float(branch_df.iloc[line_idx].get(current_col, 0.0))

            # Construct balanced 3-phase phasors
            phasors = [
                I_mag * np.exp(1j * 0),
                I_mag * np.exp(1j * -2 * np.pi / 3),
                I_mag * np.exp(1j * 2 * np.pi / 3)
            ]
            return phasors

        except Exception as e:
            logging.warning(f"measure_line_currents_phasors({line_elem_name}) failed on attempt {attempt+1}: {e}")
            time.sleep(0.5)  # wait briefly in case file is locked
            continue

    logging.error(f"measure_line_currents_phasors: Failed to read {excel_path} after multiple attempts.")
    return []



def get_bus_phase_voltages(bus_name, excel_path=EXCEL_PATH):
    """
    Robust version: reads the 'nodes' sheet using retry & validation.
    """
    max_retries = 3
    delay = 0.5

    for attempt in range(max_retries):
        try:
            if not os.path.exists(excel_path):
                logging.warning(f"get_bus_phase_voltages: Excel not found at {excel_path}")
                return []

            xls = pd.ExcelFile(excel_path, engine="openpyxl")
            sheet_names_lower = [s.lower() for s in xls.sheet_names]

            # detect nodes sheet name
            if "nodes" in sheet_names_lower:
                sheet_name = "nodes"
            else:
                possible = [s for s in xls.sheet_names if "node" in s.lower()]
                if not possible:
                    logging.warning("get_bus_phase_voltages: no sheet matching 'nodes'")
                    return []
                sheet_name = possible[0]

            df = pd.read_excel(xls, sheet_name=sheet_name, engine="openpyxl")

            if "name" not in df.columns:
                logging.warning("get_bus_phase_voltages: missing 'name' column")
                return []

            # locate the bus
            row = df[df["name"].astype(str).str.lower() == bus_name.lower()]
            if row.empty:
                return []

            row = row.iloc[0]
            v_real = float(row.get("voltage_real", 0.0))
            v_imag = float(row.get("voltage_imag", 0.0))
            v1 = complex(v_real, v_imag)
            if abs(v1) == 0:
                return []

            # generate balanced 3-phase set
            v2 = v1 * np.exp(-1j * 2 * np.pi / 3)
            v3 = v1 * np.exp(1j * 2 * np.pi / 3)
            return [v1, v2, v3]

        except Exception as e:
            logging.warning(f"get_bus_phase_voltages: attempt {attempt+1} failed: {e}")
            time.sleep(delay)
    logging.error(f"get_bus_phase_voltages: failed after {max_retries} retries")
    return []


def update_cb_status(cb_name, new_status): 
    """Update the CB status ('ON'/'OFF') in cb_states.json.""" 
    try: 
        if not os.path.exists(CB_JSON_PATH): 
            logging.warning(f"{CB_JSON_PATH} not found, creating new file.") 
            cb_states = {} 
        else: 
            with open(CB_JSON_PATH, "r") as f: 
                cb_states = json.load(f) 
                
                if cb_name in cb_states: 
                    cb_states[cb_name]["status"] = new_status 
                else: cb_states[cb_name] = {"status": new_status} 
                
                with open(CB_JSON_PATH, "w") as f: 
                    json.dump(cb_states, f, indent=2) 
                
                logging.info(f"CB {cb_name} set to {new_status} in JSON.") 
                return True 
    except Exception as e: 
        logging.error(f" Failed to update CB {cb_name} in JSON: {e}") 
        return False

def open_breaker(cb_name):
    """Simulate opening a breaker by marking it OFF in JSON."""
    return update_cb_status(cb_name, "OFF")


def close_breaker(cb_name):
    """Simulate closing a breaker by marking it ON in JSON."""
    return update_cb_status(cb_name, "ON")

def is_fault_cleared_for_line(protected_line, prefault_voltages, feeder_lines, pickup_A):
    _, line_to_buses = build_topology_from_feeder_list(feeder_lines)
    b1, b2 = line_to_buses.get(protected_line, (None, None))
    i_ph = measure_line_currents_phasors(protected_line)
    Imax = max((abs(i) for i in i_ph), default=0.0)
    v_ph = get_bus_phase_voltages(b2) if b2 else []
    vmag = (abs(v_ph[0]) if v_ph else 0.0)
    preV = prefault_voltages.get(b2, None)
    current_thresh = pickup_A * 0.5
    voltage_frac_thresh = 0.5
    vol_ok = False
    if preV and preV > 0:
        vol_ok = (vmag >= (preV * voltage_frac_thresh))
    else:
        vol_ok = True if Imax < current_thresh else False
    cleared = (Imax < current_thresh) and vol_ok
    logging.info(f"is_fault_cleared_for_line({protected_line}): Imax={Imax:.1f}A (thresh={current_thresh:.1f}), vmag={vmag:.1f} V, preV={preV}, cleared={cleared}")
    return cleared

def auto_reclose_breaker(cb_name, relays, prefault_voltages, feeder_lines, excel_path,
                         attempts=3, delays=(1, 5, 15)):
    """
    Perform sequential auto-reclose by checking bus voltages from Excel after each reclose.
    """
   
    info = relays.get(cb_name)
    if not info:
        logging.warning(f"auto_reclose_breaker: unknown breaker {cb_name}")
        return False

    protected_line = info.get('protected_line')
    pickup_A = info.get('pickup_A', 250.0)

    logging.info(f"Starting auto-reclose for {cb_name} protecting {protected_line} (max attempts={attempts})")

    for attempt in range(attempts):
        logging.info(f"Auto-reclose {cb_name}: attempt {attempt+1}/{attempts} - closing breaker")
        close_breaker(cb_name)

        # ---- Read latest voltages from Excel ----
        try:
            xls = pd.ExcelFile(excel_path)
            if 'nodes' not in xls.sheet_names:
                logging.warning(" 'nodes' sheet missing in Excel; cannot check voltages.")
                current_voltages = {}
            else:
                bus_df = pd.read_excel(xls, sheet_name='nodes')
                current_voltages = dict(zip(bus_df['name'], bus_df['voltage_pu']))
        except Exception as e:
            logging.error(f" Error reading nodes voltages from Excel: {e}")
            current_voltages = {}

        # ---- Check if fault cleared ----
        if protected_line and is_fault_cleared_for_line(protected_line, prefault_voltages, feeder_lines, pickup_A):
            logging.info(f" Reclose successful for {cb_name} on attempt {attempt+1}. Keeping breaker closed.")
            relays[cb_name]['tripped'] = False
            return True
        else:
            logging.info(f" Fault persists after reclose attempt {attempt+1} for {cb_name}. Opening breaker and waiting.")
            open_breaker(cb_name)

            wait_delay = delays[attempt] if attempt < len(delays) else delays[-1]
            logging.info(f" Waiting {wait_delay}s before next reclose attempt for {cb_name}.")
            time.sleep(wait_delay)

    logging.error(f" Auto-reclose LOCKOUT for {cb_name} after {attempts} attempts. Leaving breaker open.")
    relays[cb_name]['tripped'] = True

    # ----------------------------
    # 🔥 TRIGGER SERVICE RESTORATION HERE
    # ----------------------------
    try:
        url = "http://service_restoration:4005/optimize"      # <-- your service_restoration.py endpoint
        payload = {"breaker": cb_name, "status": "LOCKOUT"}
        requests.post(url, json=payload, timeout=20)
        logging.info(f"🚨 Service restoration triggered for {cb_name}")
    except Exception as e:
        logging.error(f"❌ Failed to trigger service restoration: {e}")

        
    return False



def run_directional_relays(relays, branch_df, node_df, dt=0.05, max_run_time=1.0):
    """
    Run directional overcurrent relay logic based on DataFrames (no OpenDSS calls).
    branch_df: DataFrame with columns ['protected_line', 'line_current_A', ...]
    node_df: DataFrame with columns ['name', 'voltage_real', 'voltage_imag']
    """

    def get_cb_status(cb_name):
        """Return current CB status ('ON' or 'OFF') from cb_states.json. Defaults to 'ON'."""
        if not os.path.exists(CB_JSON_PATH):
            return "ON"
        try:
            with open(CB_JSON_PATH, "r") as f:
                data = json.load(f)
            return data.get(cb_name, {}).get("status", "ON")
        except Exception as e:
            logging.error(f"Error reading CB state JSON: {e}")
            return "ON"

    def get_Imax_from_df(line_name, branch_df):
        """
        Get line current from branch_df without assuming a 'protected_line' column.
        Uses Line.L<number> -> zero-based index mapping.
        """
        import re
        m = re.search(r"Line\.L(\d+)", line_name)
        if not m:
            logging.warning(f"Invalid line name format: {line_name}")
            return 0.0
        line_idx = int(m.group(1)) - 1  # zero-based
        if line_idx < 0 or line_idx >= len(branch_df):
            logging.warning(f"Line index {line_idx+1} out of range for branch_df")
            return 0.0

        # Try to find current column
        current_col_candidates = [c for c in branch_df.columns if "line_current" in c.lower()]
        if not current_col_candidates:
            logging.warning("No 'line_current' column found in branch_df")
            return 0.0

        Imax = float(branch_df.iloc[line_idx][current_col_candidates[0]])
        return Imax


    def get_bus_voltage_mag(bus_name):
        row = node_df[node_df["name"].astype(str).str.lower() == bus_name.lower()]
        if row.empty:
            return 0.0
        row = row.iloc[0]
        v_real = float(row.get("voltage_real", 0.0))
        v_imag = float(row.get("voltage_imag", 0.0))
        return (v_real**2 + v_imag**2) ** 0.5

    # ---------------------
    # FAULT SECTION LOCALIZATION LOGIC
    # ---------------------
    def narrow_fault_zone(tripped_list, relays):
        """
        Ensure only the two breakers around the fault section remain OPEN.
        Others should be restored.
        """
        if len(tripped_list) < 2:
            return tripped_list  # Not enough info

        # Sort by line index
        def extract_idx(cb):
            prot = relays[cb]['protected_line']
            m = re.search(r"L(\d+)", prot)
            return int(m.group(1)) if m else 9999

        tr_sorted = sorted(tripped_list, key=extract_idx)

        # Pair-wise check of adjacent tripped CBs
        final_fault_zone = []
        for i in range(len(tr_sorted) - 1):
            cbA = tr_sorted[i]
            cbB = tr_sorted[i + 1]
            idxA = extract_idx(cbA)
            idxB = extract_idx(cbB)

            # Adjacent lines => True fault zone
            if idxB == idxA + 1:
                final_fault_zone = [cbA, cbB]

        if final_fault_zone:
            logging.warning(f"FAULT ZONE IDENTIFIED between {final_fault_zone}")
            # lock these
            for cb in final_fault_zone:
                open_breaker(cb)
                relays[cb]['tripped'] = True

            # close all others
            for cb in relays:
                if cb not in final_fault_zone:
                    close_breaker(cb)
                    relays[cb]['tripped'] = False

            return final_fault_zone

        return tripped_list


    elapsed = 0.0
    tripped_list = []

    while elapsed < max_run_time:
        for cb_name, r in relays.items():
            if get_cb_status(cb_name) == "OFF":
                logging.info(f"{cb_name} is already OFF. Skipping this cycle.")
                continue

            if r.get('tripped'):
                continue

            protected_line = r.get('protected_line')
            if not protected_line:
                continue

            pickup = r.get('pickup_A', 250.0)
            Imax = get_Imax_from_df(protected_line, branch_df)
            upstream_bus = r.get('upstream_bus', '')

            dir_ok = True  # optional: skip directional check for simplicity

            logging.info(f"Relay {cb_name} check: Imax={Imax:.1f} A (pickup={pickup:.1f}), dir_ok={dir_ok}")

            if Imax >= pickup and dir_ok:
                r['operate_time'] += dt
                logging.info(f"Relay {cb_name}: condition met, accumulated operate_time={r['operate_time']:.3f}s (delay={r['delay_s']:.3f}s)")
                if r['operate_time'] >= r['delay_s']:
                    try:
                        logging.info(f" Relay {cb_name} operating - tripping breaker.")
                        open_breaker(cb_name)
                        r['tripped'] = True
                        tripped_list.append(cb_name)
                        tripped_list = narrow_fault_zone(tripped_list, relays)
                        vmag = get_bus_voltage_mag(upstream_bus)
                        logging.info(f"Post-trip voltage at {upstream_bus}: {vmag:.2f} V")
                    except Exception as e:
                        logging.error(f"Failed to trip {cb_name}: {e}")
            else:
                if r['operate_time'] > 0:
                    logging.info(f"Relay {cb_name}: condition cleared/reset (operate_time was {r['operate_time']:.3f}s).")
                r['operate_time'] = 0.0
        
        
        if tripped_list:
            break
        elapsed += dt
        time.sleep(dt)
    
    return tripped_list



# ---------------- Original test1.py utility functions ----------------

def load_excel_with_retry(path, max_retries=MAX_RETRIES, delay=RETRY_DELAY_SEC):
    for attempt in range(1, max_retries + 1):
        try:
            # Validate extension and integrity before loading
            if not os.path.exists(path) or not path.endswith(".xlsx"):
                raise FileNotFoundError(f"{path} not found or not an .xlsx file")

            # Attempt to open Excel file
            return pd.ExcelFile(path, engine="openpyxl")

        except Exception as e:
            logging.warning(f" Attempt {attempt} failed to load Excel: {e}")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                logging.error(f" Failed to load Excel after {max_retries} attempts.")
                raise

# ---------------- Rebuild + protection orchestration ----------------
def rebuild_circuit_from_excel(run_relays=True):
    """
    Reads exported Excel grid data, runs relay + auto-reclosure logic (if enabled),
    and updates CB status back to Excel. Does not rebuild or simulate the circuit.
    """
    logging.info(" Reading Excel grid data for relay and auto-reclosure decisions...")

    try:
        xls = load_excel_with_retry(EXCEL_PATH)
        node_df = pd.read_excel(xls, sheet_name="nodes")
        branch_df = pd.read_excel(xls, sheet_name="branches")
        # --- DEBUG: check max line current from Excel ---
        Imax = branch_df["line_current_A"].max()
        print("Imax:", Imax)
        logging.info(f"DEBUG: Imax from branches sheet = {Imax:.2f} A")

        cb_df = pd.read_excel(xls, sheet_name="CB")
    except Exception as e:
        logging.error(f" Could not read Excel: {e}")
        return

    # --- Extract pre-fault voltage and current references directly from Excel ---
    prefault_voltages = {}
    for _, row in node_df.iterrows():
        bus_name = str(row.get("name", "")).strip()
        if bus_name:
            try:
                v_real = float(row.get("voltage_real", 0.0))
                v_imag = float(row.get("voltage_imag", 0.0))
                prefault_voltages[bus_name] = (v_real**2 + v_imag**2) ** 0.5
            except Exception:
                continue

    prefault_currents = {}
    for _, row in branch_df.iterrows():
        try:
            line_name = f"Line.{row.name + 1}"
            prefault_currents[line_name] = float(row.get("line_current_A", 0.0))
        except Exception:
            continue

    # --- Extract feeder topology info for directional relay coordination ---
    feeder_lines = []
    for _, row in branch_df.iterrows():
        try:
            from_bus = int(re.sub(r'\D', '', str(row.get("from", ""))))
            to_bus = int(re.sub(r'\D', '', str(row.get("to", ""))))
            r = float(row.get("r", 0))
            x = float(row.get("x", 0))
            feeder_lines.append((from_bus, to_bus, r, x))
        except Exception:
            continue

    # --- Build breaker map from Excel (used for relay mapping) ---
    breaker_map = {}
    for _, row in cb_df.iterrows():
        cb_name = str(row.get("cb_name", "")).strip()
        if not cb_name:
            continue
        protected_line_raw = str(row.get("protected_line", "")).strip()
        mapped_line = map_cb_to_branch_line(protected_line_raw, branch_df)  # <-- map here
        breaker_map[cb_name] = {
            "upstream_bus": str(row.get("upstream_bus", "")).strip(),
            "downstream_bus": str(row.get("downstream_bus", "")).strip(),
            "protected_line": mapped_line,
            "status": str(row.get("status", "closed")).lower()
        }


    # --- Run relay + auto-reclosure logic (using data from auto_reclosure.py) ---
    if run_relays:
        logging.info(" Setting up directional relays using Excel data...")
    
        relays = setup_directional_relays(
            breaker_map, prefault_currents, feeder_lines,
            pickup_mult=3.0, min_pickup_A=250.0,
            min_delay=0.05, max_delay=0.6
        )

        logging.info(" Running DOCR + Time Grading logic...")
        tripped = run_directional_relays(relays, branch_df, node_df, dt=0.05, max_run_time=1.0)

        if tripped:
            logging.info(f" Breakers tripped: {tripped}")
            for cb in tripped:
                success = auto_reclose_breaker(
                    cb,                       # pass the tripped breaker name
                    relays,
                    prefault_voltages,
                    feeder_lines,
                    excel_path=EXCEL_PATH
                )

                cb_df.loc[cb_df["cb_name"] == cb, "status"] = "closed" if success else "open"
                cb_df.loc[cb_df["cb_name"] == cb, "auto_reclose_result"] = "SUCCESS" if success else "LOCKOUT"
                logging.info(f"Auto-reclose result for {cb}: {'SUCCESS' if success else 'LOCKOUT'}")
        else:
            logging.info(" No breakers tripped within the current cycle.")


# ================= Periodic Task =================
def periodic_task():
    while True:
        rebuild_circuit_from_excel(run_relays=True)
        logging.info("⏱ Sleeping 2 minutes before next rebuild...")
        for handler in logger.handlers:
            handler.flush()
        time.sleep(120)

# ================= Flask Health Endpoint =================
app = Flask(__name__)

@app.route("/health")
def health():
    return "🟢 Grid parser with relays running", 200

# ================= Main =================
if __name__ == "__main__":
    rebuild_thread = threading.Thread(target=periodic_task, daemon=False)
    rebuild_thread.start()
    logging.info("🚀 Flask API running on port 4007...")
    app.run(host="0.0.0.0", port=4007, debug=False, use_reloader=False)
