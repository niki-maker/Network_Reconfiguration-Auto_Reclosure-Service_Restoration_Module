import opendssdirect as dss
import pandas as pd
import logging
import os
import time
import json
import threading
from flask import Flask, request, jsonify
from collections import deque
import re
from datetime import datetime

# ================= Logging Setup =================
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
logger.addHandler(logging.FileHandler("service_restoration.log"))
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

EXCEL_PATH = "/shared_volume/grid_data1.xlsx"
CB_STATE_PATH = "/shared_volume/cb_states.json"
ACTIVATED_DEVICES_PATH = "/shared_volume/activated_devices.json"
COMP_DEVICE_PATH = "/app/data/compensator_device.json"

MAX_RETRIES = 3
RETRY_DELAY_SEC = 2

app = Flask(__name__)


# ----------------- Helper Functions -----------------
def load_json(path):
    if os.path.exists(path):
        with open(path, "r") as f:
            return json.load(f)
    return {}


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def load_excel_with_retry(path):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return pd.ExcelFile(path, engine="openpyxl")
        except Exception as e:
            logger.warning(f"Attempt {attempt} failed to load Excel: {e}")
            time.sleep(RETRY_DELAY_SEC)
    raise FileNotFoundError(f"Failed to load Excel after {MAX_RETRIES} attempts")


def map_cb_to_branch_line(cb_protected_line, branch_df):
    if not cb_protected_line:
        return None
    m = re.search(r"Line\.L_CB(\d+)", cb_protected_line)
    if m:
        idx = int(m.group(1))
        if 0 < idx <= len(branch_df):
            return f"Line.L{idx}"
    return cb_protected_line


def build_topology(feeder_lines):
    adj = {}
    line_to_buses = {}
    for idx, (b1, b2, _, _) in enumerate(feeder_lines, start=1):
        l_name = f"Line.L{idx}"
        bus1 = f"bus{b1}"
        bus2 = f"bus{b2}"
        line_to_buses[l_name] = (bus1, bus2)
        adj.setdefault(bus1, []).append(bus2)
    return adj, line_to_buses


def bfs_downstream(adj, start_bus):
    visited = set()
    queue = [start_bus]
    while queue:
        b = queue.pop(0)
        if b in visited:
            continue
        visited.add(b)
        for nb in adj.get(b, []):
            if nb not in visited:
                queue.append(nb)
    return visited


def create_grid_tie_line(bus1, bus2, r=0.0, x=0.0):
    line_name = f"{bus1}-{bus2}_CB".replace('-', '_')
    try:
        dss.Text.Command(
            f"New Line.{line_name} Bus1={bus1}.1.2.3 Bus2={bus2}.1.2.3 "
            f"Phases=3 R1={r} X1={x} C1=0 Enabled=Yes"
        )
        logger.info(f"Grid-tie line created: {line_name} ({bus1} ↔ {bus2})")
        return line_name
    except Exception as e:
        logger.error(f"Failed to create grid-tie line {line_name}: {e}")
        return None


def install_circuit_breakers(feeder_lines, grid_ties=None):
    logging.info("\n Installing circuit breakers for feeders and grid-ties...")
    breaker_map = {}

    if grid_ties is None:
        grid_ties = []

    # Disable existing lines
    dss.Lines.First()
    while True:
        name = dss.Lines.Name()
        if not name:
            break
        dss.Text.Command(f"Edit Line.{name} Enabled=no")
        if not dss.Lines.Next():
            break

    index_counter = 1

    # Feeder lines
    for b1, b2, r1, x1 in feeder_lines:
        cb_name = f"CB{index_counter}"
        bus_up = f"bus{b1}"
        mid_bus = f"bus{b1}_CB"
        prot_line = f"L_CB{index_counter}"

        dss.Text.Command(f"New Line.{cb_name} Bus1={bus_up} Bus2={mid_bus} Phases=3 R1=1e-6 X1=1e-7 Switch=True")
        dss.Text.Command(f"New Line.{prot_line} Bus1={mid_bus} Bus2=bus{b2} Phases=3 R1={r1} X1={x1} C1=0.0")

        breaker_map[cb_name] = {
            "upstream_bus": bus_up,
            "mid_bus": mid_bus,
            "downstream_bus": f"bus{b2}",
            "protected_line": f"Line.{prot_line}",
            "line_index": index_counter,
        }

        index_counter += 1

    # Grid ties
    for tie_name, (bus1, bus2, r, x) in grid_ties:
        cb_name = tie_name.replace("-", "_")
        mid_bus = f"{bus1}_CB"
        prot_line = f"{cb_name}_line"

        dss.Text.Command(f"New Line.{cb_name} Bus1={bus1} Bus2={mid_bus} Phases=3 R1=1e-6 X1=1e-7 Switch=True")
        dss.Text.Command(f"New Line.{prot_line} Bus1={mid_bus} Bus2={bus2} Phases=3 R1={r} X1={x}")

        breaker_map[cb_name] = {
            "upstream_bus": bus1,
            "mid_bus": mid_bus,
            "downstream_bus": bus2,
            "protected_line": f"Line.{prot_line}",
            "line_index": index_counter,
        }

        index_counter += 1

    dss.Solution.Solve()
    logging.info(f"Installed {len(breaker_map)} breakers.")

    return breaker_map


def sync_cb_states(breaker_map):
    if os.path.exists(CB_STATE_PATH):
        try:
            with open(CB_STATE_PATH, "r") as f:
                old_states = json.load(f)
        except:
            old_states = {}
    else:
        old_states = {}

    for name, info in breaker_map.items():
        info["status"] = old_states.get(name, {}).get("status", "ON")

    with open(CB_STATE_PATH, "w") as f:
        json.dump(breaker_map, f, indent=2)

    return breaker_map


def extract_feeder_lines_from_excel(path):
    df = pd.read_excel(path, sheet_name="branches")

    bus_pattern = re.compile(r"^bus(\d+)$")
    feeder_lines = []

    for _, row in df.iterrows():
        from_bus = str(row["from"]).strip()
        to_bus = str(row["to"]).strip()

        m1 = bus_pattern.match(from_bus)
        m2 = bus_pattern.match(to_bus)
        if not (m1 and m2):
            continue

        bus1 = int(m1.group(1))
        bus2 = int(m2.group(1))
        r = float(row["r"])
        x = float(row["x"])

        feeder_lines.append((bus1, bus2, r, x))

    return feeder_lines


# ------------------------------------------------------------
# REBUILD IEEE-33
# ------------------------------------------------------------
def build_ieee33_system_from_excel():
    try:
        if not os.path.exists(EXCEL_PATH):
            logging.error(f"Excel file not found: {EXCEL_PATH}")
            return

        xls = load_excel_with_retry(EXCEL_PATH)
        dss.Text.Command("Clear")
        dss.Basic.ClearAll()
        dss.Text.Command("New Circuit.RebuiltIEEE33 basekv=12.66 pu=1.0 phases=3 bus1=bus1")
        dss.Text.Command("Edit Vsource.Source bus1=bus1 phases=3 pu=1.0 basekv=12.66 angle=0")

        feeder_lines = extract_feeder_lines_from_excel(EXCEL_PATH)

        for idx, (b1, b2, r, x) in enumerate(feeder_lines, start=1):
            dss.Text.Command(
                f"New Line.L{idx} Bus1=bus{b1} Bus2=bus{b2} Phases=3 R1={r} X1={x} C1=0.0"
            )

        # Loads
        load_df = pd.read_excel(xls, sheet_name="loads")
        for i, row in load_df.iterrows():
            mult = row.get("load_multiplier", 1.0)
            dss.Text.Command(
                f"New Load.L{i+1} Bus1={row['bus']}.1.2.3 Phases=3 Conn=wye Model=1 "
                f"kW={row['kW']*mult} kvar={row['kvar']*mult} kv=12.66"
            )

        # Capacitors
        cap_df = pd.read_excel(xls, sheet_name="capacitors")
        for i, row in cap_df.iterrows():
            dss.Text.Command(
                f"New Capacitor.{row['name']} Bus1={row['bus']} Phases={row['phases']} "
                f"kVAR={row['kVAR']} kV={row['kV']}"
            )

        # Point: activated devices
        activated_devices = load_json(ACTIVATED_DEVICES_PATH)
        comp_data = load_json(COMP_DEVICE_PATH)

        # Activate capacitors
        for cap_name in activated_devices.get("capacitors", []):
            kvar_val = comp_data.get("capacitor_reactive_power", {}).get(cap_name, 100)
            dss.Text.Command(
                f"New Capacitor.{cap_name} Bus1={cap_name} Phases=3 kVAR={kvar_val} kV=12.66"
            )

        # Activate reactors
        for reac_name in activated_devices.get("reactors", []):
            kvar_val = comp_data.get("shunt_reactor_reactive_power", {}).get(reac_name, 500)
            dss.Text.Command(
                f"New Reactor.{reac_name} Bus1={reac_name} Phases=3 kVAR={kvar_val} kV=12.66"
            )

        # Activate grid ties
        activated_ties = []
        for tie in activated_devices.get("grid_ties", []):
            bus1, bus2 = tie.split("-")
            if tie in comp_data.get("tie_impedance", {}):
                r, x = comp_data["tie_impedance"][tie]
                activated_ties.append((f"{tie}_CB", (bus1, bus2, r, x)))

        # Install CBs
        breaker_map = install_circuit_breakers(feeder_lines, activated_ties)
        breaker_map = sync_cb_states(breaker_map)

        # Set CB states
        for cb_name, info in breaker_map.items():
            status = info.get("status", "ON")
            try:
                dss.Circuit.SetActiveElement(f"Line.{cb_name}")
                dss.CktElement.Enabled(status == "ON")
            except:
                pass

        # Solve
        dss.Text.Command("Set Voltagebases=[12.66]")
        dss.Text.Command("CalcVoltageBases")
        dss.Text.Command("Solve")

        logging.info(f"IEEE33 Rebuilt @ {datetime.now().strftime('%H:%M:%S')}")

        # ---------------------------------------------------------
        # 7. BUILD NETWORK GRAPH  (RETURN THIS!)
        # ---------------------------------------------------------
        graph = {}

        # Add simple connectivity graph from feeder lines
        for b1, b2, _, _ in feeder_lines:
            graph.setdefault(f"bus{b1}", []).append(f"bus{b2}")
            graph.setdefault(f"bus{b2}", []).append(f"bus{b1}")

        # Add grid-tie connections
        for tie_name, (bus1, bus2, _, _) in activated_ties:
            graph.setdefault(bus1, []).append(bus2)
            graph.setdefault(bus2, []).append(bus1)

        # Add breaker connectivity (CB as nodes if you want)
        for cb_name, info in breaker_map.items():
            up = info.get("upstream_bus")
            dn = info.get("downstream_bus")
            if up and dn:
                graph.setdefault(up, []).append(dn)
                graph.setdefault(dn, []).append(up)

        return graph

    except Exception as e:
        logging.exception(f"Error rebuilding circuit: {e}")
        return None

def select_tie_switch(graph, downstream_segment, tie_switches):
    """
    graph: adjacency list from build_ieee33_system_from_excel()
    downstream_segment: set of isolated buses
    tie_switches: list of [busA, busB]
    """

    # Load activated ties
    activated_devices = load_json(ACTIVATED_DEVICES_PATH)
    already_active = set(activated_devices.get("grid_ties", []))

    found_any_valid = False  # tracks whether ANY tie is structurally valid

    # 1. Compute energized region
    energized = set()
    queue = ["bus1"]
    visited = set()

    while queue:
        node = queue.pop(0)
        if node in visited:
            continue
        visited.add(node)

        if node not in downstream_segment:
            energized.add(node)
            for nbr in graph.get(node, []):
                queue.append(nbr)

    # 2. Scan all tie switches
    for busA, busB in tie_switches:

        tie_name = f"{busA}-{busB}"

        cond1 = (busA in downstream_segment and busB in energized)
        cond2 = (busB in downstream_segment and busA in energized)

        is_valid = cond1 or cond2
        logger.info(f"Checking tie {busA}-{busB} :: {is_valid}")

        # Record that a valid tie exists (even if activated)
        if is_valid:
            found_any_valid = True

            # Valid but already active
            if tie_name in already_active:
                logger.info("A valid tie exists but is already energized. No new tie needed.")
                return None, None

            # Valid and not active → select it
            logger.info(f"Selected Tie Switch: {busA} ↔ {busB}")
            return busA, busB

    # If ZERO ties were valid → only then show warning
    if not found_any_valid:
        logger.warning("No valid tie switch found for service restoration")

    return None, None

def load_json_safe(path, default_data):
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except:
            return default_data
    return default_data

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

# ------------------------------------------------------------
# SERVICE RESTORATION ENGINE
# ------------------------------------------------------------
def service_restoration():
    try:
        xls = load_excel_with_retry(EXCEL_PATH)
        branch_df = pd.read_excel(xls, sheet_name="branches")
        cb_df = pd.read_excel(xls, sheet_name="CB")
        comp_data = load_json(COMP_DEVICE_PATH)
        activated_devices = load_json(ACTIVATED_DEVICES_PATH)
        cb_states = load_json(CB_STATE_PATH)

        # Build circuit
        build_ieee33_system_from_excel()

        # Feeder lines
        feeder_lines = []
        for _, row in branch_df.iterrows():
            try:
                b1 = int(re.sub(r"\D", "", str(row["from"])))
                b2 = int(re.sub(r"\D", "", str(row["to"])))
                r = float(row["r"])
                x = float(row["x"])
                feeder_lines.append((b1, b2, r, x))
            except:
                continue

        # Breaker map
        breaker_map = {}
        for _, row in cb_df.iterrows():
            cb_name = str(row["cb_name"]).strip()
            if not cb_name:
                continue
            mapped_line = map_cb_to_branch_line(str(row["protected_line"]).strip(), branch_df)
            breaker_map[cb_name] = {
                "upstream_bus": row["upstream_bus"],
                "downstream_bus": row["downstream_bus"],
                "protected_line": mapped_line,
                "status": cb_states.get(cb_name, {}).get("status", "ON"),
            }

        # Detect all OFF circuit breakers
        fault_cbs = []

        for cb_name, info in breaker_map.items():
            if str(info["status"]).upper() == "OFF":
                fault_cbs.append(cb_name)

        if not fault_cbs:
            logger.info("No fault detected.")
            return

        logger.info(f"Fault breakers detected: {fault_cbs}")

        # ------------------------------------------
        # CASE 2: More than one breaker is OFF → RING
        # ------------------------------------------
        if len(fault_cbs) > 1:

            logger.info("Detected ring fault scenario (multiple breakers tripped).")

            activated_ties = activated_devices.get("grid_ties", [])
            if not activated_ties:
                logger.info("No activated grid ties present.")
            else:
                logger.info(f"Checking activated grid ties: {activated_ties}")

                # Load SR file (new file)
                SR_FILE = "/shared_volume/SR_activated_grid_tie.json"
                sr_data = load_json_safe(SR_FILE, {"grid_ties": []})

                # Build topology and energized region
                adj, _ = build_topology(feeder_lines)
                queue, visited = ["bus1"], set()
                while queue:
                    node = queue.pop(0)
                    if node not in visited:
                        visited.add(node)
                        for n in adj.get(node, []):
                            queue.append(n)
                energized_region = visited

                ties_to_move = []  # ties to move into SR file

                # Check which grid-ties energize OFF breakers
                for tie in activated_ties:
                    try:
                        busA, busB = tie.split("-")
                    except:
                        continue

                    energizes = []

                    for cb in fault_cbs:
                        down_bus = breaker_map[cb]["downstream_bus"]
                        cb_downstream = bfs_downstream(adj, down_bus)

                        condA = (busA in cb_downstream and busB in energized_region)
                        condB = (busB in cb_downstream and busA in energized_region)

                        if condA or condB:
                            energizes.append(cb)

                    if energizes:
                        logger.info(f"Grid-tie {tie} energizes faulty breakers: {energizes}")
                        ties_to_move.append(tie)

                # ---------------------------
                # Move ties → SR file
                # ---------------------------
                if ties_to_move:
                    for tie in ties_to_move:
                        # Remove from activated_devices.json
                        if tie in activated_devices["grid_ties"]:
                            activated_devices["grid_ties"].remove(tie)

                        # Add to SR file if not already present
                        if tie not in sr_data["grid_ties"]:
                            sr_data["grid_ties"].append(tie)

                    # Save both files
                    save_json(ACTIVATED_DEVICES_PATH, activated_devices)
                    save_json(SR_FILE, sr_data)

                    logger.info(f"Moved {ties_to_move} → SR_activated_grid_tie.json")

            logger.info("Ring fault restoration complete (no new tie switching).")
            return

        # ------------------------------------------
        # CASE 1: Single breaker OFF → RADIAL FAULT
        # ------------------------------------------
        fault_cb = fault_cbs[0]
        logger.info(f"Primary fault breaker: {fault_cb}")

        # Compute downstream region from the single fault CB
        adj, _ = build_topology(feeder_lines)
        downstream = bfs_downstream(adj, breaker_map[fault_cb]["downstream_bus"])
        logger.info(f"Downstream buses: {downstream}")

        # Build graph for tie-switch selection
        graph = build_ieee33_system_from_excel()

        # Find NEXT downstream breaker
        next_cb = None
        for cb_name, info in breaker_map.items():
            if cb_name == fault_cb:
                continue
            if info["upstream_bus"] in downstream:
                next_cb = cb_name
                break

        # -----------------------------------------------------
        #  NEW: Mark next downstream breaker as OFF in JSON
        # -----------------------------------------------------
        if next_cb:
            logger.info(f"Next downstream breaker after {fault_cb}: {next_cb} "
                        f"(at bus {breaker_map[next_cb]['upstream_bus']})")

            # Update CB_states.json
            if next_cb in cb_states:
                cb_states[next_cb]["status"] = "OFF"

                with open(CB_STATE_PATH, "w") as f:
                    json.dump(cb_states, f, indent=4)

                logger.info(f"Updated CB_states.json → marked {next_cb} as OFF")

        else:
            logger.info(f"No downstream breaker found after {fault_cb}.")

        # -----------------------------------------------------
        # 3. SELECT TIE SWITCH (unchanged logic)
        # -----------------------------------------------------
        busA, busB = select_tie_switch(
            graph=graph,
            downstream_segment=downstream,
            tie_switches=comp_data["tie_switches"]
        )

        if busA is not None and busB is not None:
            tie_name = f"{busA}-{busB}"

            logger.info(f"Selected Tie: {busA} ↔ {busB}")
            logger.info(f"Grid tie {tie_name} can perform service restoration")

        # Ensure keys exist
        activated_devices.setdefault("grid_ties", [])

        SR_FILE = "/shared_volume/SR_activated_grid_tie.json"
        if os.path.exists(SR_FILE):
            with open(SR_FILE, "r") as f:
                sr_data = json.load(f)
        else:
            sr_data = {"grid_ties": []}

        sr_data.setdefault("grid_ties", [])

        # ADD TO SR_activated_grid_tie.json
        if tie_name not in sr_data["grid_ties"]:
            sr_data["grid_ties"].append(tie_name)
            with open(SR_FILE, "w") as f:
                json.dump(sr_data, f, indent=4)
            logger.info(f"SR_activated_grid_tie.json → added {tie_name}")
        else:
            logger.info(f"{tie_name} already present in SR_activated_grid_tie.json")

        # ----------------------------------------------------------
        # ACTIVATE TIE IN DSS
        # ----------------------------------------------------------
        r, x = comp_data["tie_impedance"].get(tie_name, (0, 0))
        create_grid_tie_line(busA, busB, r, x)

        return {"status": "Service restoration complete"}





    except Exception as e:
        logger.error(f"Error in service restoration: {e}")
        return {"status": "Error"}



# ---------------- Flask Endpoint ----------------
@app.route("/optimize", methods=["POST"])
def optimize_endpoint():
    return jsonify(service_restoration())

# ---------------- Run App ----------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=4005)
