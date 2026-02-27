import opendssdirect as dss
import logging
import pandas as pd
import numpy as np
import uuid
import os
import time
import threading
import json
from flask import Flask, jsonify

app = Flask(__name__)

# Logging setup
logging.basicConfig(filename="base_network.log", level=logging.INFO)
logging.info("🔁 Starting grid parser in base_network.py...")
os.makedirs("/shared_volume", exist_ok=True)

# Path constants
ACTIVATED_DEVICES_PATH = "/shared_volume/activated_devices.json"
COMPENSATOR_DEVICE_PATH = "/app/data/compensator_device.json"
CB_STATE_PATH = "/shared_volume/cb_states.json"
SR_FILE = "/shared_volume/SR_activated_grid_tie.json"

# Prevent multiple overlapping scheduled load-change threads
load_change_scheduled = False

# ──────────────────────────────────────────────
# Classes
# ──────────────────────────────────────────────
class Node:
    def __init__(self, name, uuid, voltage_pu, power_pu, base_voltage, base_apparent_power,
                 real_power, imag_power, voltage_real, voltage_imag, load_multiplier=1.0):
        self.name = name
        self.uuid = uuid
        self.voltage_pu = voltage_pu
        self.power_pu = power_pu
        self.baseVoltage = base_voltage
        self.base_apparent_power = base_apparent_power
        self.power = complex(real_power, imag_power)
        self.voltage = complex(voltage_real, voltage_imag)
        self.load_multiplier = load_multiplier


class Branch:
    def __init__(self, uuid, start_node, end_node, r, x, bch, bch_pu, length,
                 base_voltage, base_apparent_power, r_pu, x_pu, z, z_pu, type_):
        self.uuid = uuid
        self.start_node = start_node
        self.end_node = end_node
        self.r = r
        self.x = x
        self.bch = bch
        self.bch_pu = bch_pu
        self.length = length
        self.baseVoltage = base_voltage
        self.base_apparent_power = base_apparent_power
        self.r_pu = r_pu
        self.x_pu = x_pu
        self.z = z
        self.z_pu = z_pu
        self.type = type_


class GridSystem:
    def __init__(self):
        self.nodes = []
        self.branches = []
        self.loads = []
        self.capacitors = []  # NEW

# ──────────────────────────────────────────────
# Excel Export
# ──────────────────────────────────────────────
def export_grid_to_excel(system, breaker_map=None, cb_currents=None, path="grid_data1.xlsx"):
    try:
        # --- Nodes ---
        node_records = []
        for node in system.nodes:
            node_records.append({
                "name": node.name,
                "uuid": node.uuid,
                "voltage_pu": abs(node.voltage_pu),
                "voltage_angle_deg": np.angle(node.voltage_pu, deg=True),
                "power_pu": abs(node.power_pu),
                "power_angle_deg": np.degrees(np.angle(node.power_pu)),
                "base_voltage": node.baseVoltage,
                "base_apparent_power": node.base_apparent_power,
                "real_power": node.power.real,
                "imag_power": node.power.imag,
                "voltage_real": node.voltage.real,
                "voltage_imag": node.voltage.imag,
                "load_multiplier": node.load_multiplier
            })
        node_df = pd.DataFrame(node_records)

        # --- Branches ---
        branch_records = []

        # ✅ Build mapping of feeder current from cb_currents
        # Build mapping of feeder current from cb_currents (normalized)
        feeder_current_map = {}
        if cb_currents:
            for cb_name, data in cb_currents.items():
                if cb_name not in breaker_map:
                    continue

                upstream = str(breaker_map[cb_name]["upstream_bus"]).strip().lower()
                downstream = str(breaker_map[cb_name]["downstream_bus"]).strip().lower()

                current = float(data.get("line_current_A", 0.0))
                direction = data.get("current_direction", "N/A")

                # Accept both _cb and _CB variants + normalize
                key_pairs = [
                    (upstream, downstream),
                    (upstream, f"{upstream}_cb"),
                    (f"{upstream}_cb", downstream),
                    (upstream, f"{upstream}_CB"),
                    (f"{upstream}_CB", downstream),
                ]

                for key_pair in key_pairs:
                    feeder_current_map[(key_pair[0].lower(), key_pair[1].lower())] = {
                        "line_current_A": current,
                        "current_direction": direction
                    }


        for branch in system.branches:
            f = branch.start_node.name if branch.start_node else "Unknown"
            t = branch.end_node.name if branch.end_node else "Unknown"

            # Default values
            line_current = 0.0
            direction = "N/A"

            # ✅ Direct match from prebuilt map
            if (f, t) in feeder_current_map:
                line_current = feeder_current_map[(f, t)]["line_current_A"]
                direction = feeder_current_map[(f, t)]["current_direction"]
            else:
                # --- Fallback: remove '_cb' and check base feeder
                f_base = f.replace("_cb", "")
                t_base = t.replace("_cb", "")
                if (f_base, t_base) in feeder_current_map:
                    line_current = feeder_current_map[(f_base, t_base)]["line_current_A"]
                    direction = feeder_current_map[(f_base, t_base)]["current_direction"]

            branch_records.append({
                "uuid": branch.uuid,
                "from": f,
                "to": t,
                "r": branch.r,
                "x": branch.x,
                "bch": branch.bch,
                "bch_pu": branch.bch_pu,
                "length": branch.length,
                "base_voltage": branch.baseVoltage,
                "base_apparent_power": branch.base_apparent_power,
                "r_pu": branch.r_pu,
                "x_pu": branch.x_pu,
                "z_real": branch.z.real,
                "z_imag": branch.z.imag,
                "z_pu_real": branch.z_pu.real,
                "z_pu_imag": branch.z_pu.imag,
                "type": branch.type,
                "line_current_A": line_current,
                "current_direction": direction
            })

        branch_df = pd.DataFrame(branch_records)




        # --- Loads ---
        load_df = pd.DataFrame(system.loads)

        # --- Capacitors ---
        cap_df = pd.DataFrame(system.capacitors)

        # --- Circuit Breakers + Auto-Reclose Info ---
        if breaker_map:
            cb_records = []

            for cb_name, info in breaker_map.items():
                try:
                    # Activate breaker element in OpenDSS
                    dss.Circuit.SetActiveElement(f"Line.{cb_name}")

                    # Line current (Imax)
                    currents = np.array(dss.CktElement.CurrentsMagAng())
                    line_current = max(currents[::2]) if len(currents) >= 2 else 0.0

                    # Voltages
                    V = np.array(dss.CktElement.Voltages())
                    preV = abs(V[0] + 1j * V[1]) if len(V) >= 2 else 0.0
                    vmag = preV

                    # Power flow direction & average real power
                    I = np.array(dss.CktElement.Currents())
                    nph = int(dss.CktElement.NumPhases())
                    if len(V) >= 2*nph and len(I) >= 2*nph:
                        Vph = V[0:2*nph:2] + 1j*V[1:2*nph:2]
                        Iph = I[0:2*nph:2] + 1j*I[1:2*nph:2]
                        S = Vph * np.conj(Iph)
                        avg_real_power = np.mean(np.real(S))
                        dir_ok = avg_real_power > 0
                    else:
                        avg_real_power = 0.0
                        dir_ok = False

                    # Thresholds
                    pickup_A = round(max(line_current * 3.0, 100.0), 2)
                    current_thresh = round(pickup_A * 0.5, 2)
                    voltage_frac_thresh = round(0.5 * preV, 2)

                    # Append breaker info
                    cb_records.append({
                        "cb_name": cb_name,
                        "upstream_bus": info.get("upstream_bus", ""),
                        "downstream_bus": info.get("downstream_bus", ""),
                        "protected_line": info.get("protected_line", ""),
                        "line_index": info.get("line_index", ""),
                        "status": info.get("status", "UNKNOWN"),
                        "line_current_A": line_current,
                        "pickup_A": pickup_A,
                        "current_thresh": current_thresh,
                        "preV": preV,
                        "vmag": vmag,
                        "voltage_frac_thresh": voltage_frac_thresh,
                        "avg_real_power_W": avg_real_power,
                        "dir_ok": dir_ok
                    })

                except Exception as e:
                    logging.warning(f"Could not compute metrics for {cb_name}: {e}")

            cb_df = pd.DataFrame(cb_records)
        else:
            cb_df = pd.DataFrame()


        # --- Write to Excel ---
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            node_df.to_excel(writer, sheet_name="nodes", index=False)
            branch_df.to_excel(writer, sheet_name="branches", index=False)
            load_df.to_excel(writer, sheet_name="loads", index=False)
            cap_df.to_excel(writer, sheet_name="capacitors", index=False)
            cb_df.to_excel(writer, sheet_name="CB", index=False)

        logging.info(f"📁 Excel exported successfully → {path}")
        return True
    except Exception as e:
        logging.error(f"❌ Excel export failed: {e}")
        return False


# ──────────────────────────────────────────────
# Helper: Print Bus Voltages
# ──────────────────────────────────────────────

def log_cb_currents(breaker_map, title="CB Currents"):
    logging.info(f"\n=== {title.upper()} ===")
    logging.info(f"{'CB Name':<10} {'Imax (A)':>15} {'Direction':>15}")
    logging.info("-" * 45)
    cb_currents = {}

    for cb_name in breaker_map.keys():
        try:
            dss.Circuit.SetActiveElement(f"Line.{cb_name}")
            currents = np.array(dss.CktElement.CurrentsMagAng())

            if len(currents) == 0:
                logging.info(f"{cb_name:<10} {'0.0000':>15} {'N/A':>15}")
                cb_currents[cb_name] = {"line_current_A": 0.0, "current_direction": "N/A"}
                continue

            # --- Compute Imax (max current magnitude) ---
            Imax = max(currents[::2])

            # --- Compute direction (based on real power flow) ---
            V = np.array(dss.CktElement.Voltages())
            I = np.array(dss.CktElement.Currents())
            nph = int(dss.CktElement.NumPhases())

            if len(V) >= 2 * nph and len(I) >= 2 * nph:
                Vph = V[0:2*nph:2] + 1j * V[1:2*nph:2]
                Iph = I[0:2*nph:2] + 1j * I[1:2*nph:2]
                S = Vph * np.conj(Iph)
                Pavg = np.mean(np.real(S))
                direction = "Forward" if Pavg > 0 else "Reverse"
            else:
                direction = "N/A"

            logging.info(f"{cb_name:<10} {Imax:>15.4f} {direction:>15}")
            cb_currents[cb_name] = {"line_current_A": Imax, "current_direction": direction}

        except Exception as e:
            logging.warning(f"Could not log current for {cb_name}: {e}")

    return cb_currents

def log_bus_voltages(title="Bus Voltages"):
    logging.info(f"\n=== {title.upper()} ===")
    logging.info(f"{'Bus':<8} {'Voltage (p.u.)':>15} {'Angle (deg)':>15}")
    logging.info("-" * 40)
    for bus in dss.Circuit.AllBusNames():
        dss.Circuit.SetActiveBus(bus)
        volts = dss.Bus.Voltages()
        if not volts:
            continue
        v_complex = volts[0] + 1j * volts[1]
        kv_base = dss.Bus.kVBase()
        if kv_base == 0:
            continue
        pu_voltage = v_complex / (kv_base * 1000)
        angle_deg = np.angle(pu_voltage, deg=True)
        logging.info(f"{bus:<8} {abs(pu_voltage):>15.4f} {angle_deg:>15.2f}")

def install_circuit_breakers(feeder_lines, activated_ties=None, activated_ties_SR=None):
    """
    Install CBs for:
      1) feeder lines
      2) activated grid-ties
      3) service-restoration grid-ties
    """
    logging.info("\n🔌 Installing circuit breakers for feeders + activated grid-ties + SR grid-ties...")
    breaker_map = {}

    if activated_ties is None:
        activated_ties = []
    if activated_ties_SR is None:
        activated_ties_SR = []

    # Merge all grid-ties together
    all_grid_ties = activated_ties + activated_ties_SR

    # -------- disable all existing DSS lines ----------
    dss.Lines.First()
    while True:
        name = dss.Lines.Name()
        if not name:
            break
        dss.Text.Command(f"Edit Line.{name} Enabled=no")
        if not dss.Lines.Next():
            break

    index_counter = 1

    # -------------------------------------------------
    # 1️⃣ FEEDER LINES
    # -------------------------------------------------
    for b1, b2, r1, x1 in feeder_lines:

        cb_name = f"CB{index_counter}"
        bus_up = f"bus{b1}"
        mid_bus = f"bus{b1}_CB"
        prot_line = f"L_CB{index_counter}"

        # CB switch
        dss.Text.Command(
            f"New Line.{cb_name} Bus1={bus_up} Bus2={mid_bus} "
            f"Phases=3 R1=1e-6 X1=1e-7 Switch=True"
        )

        # Protected feeder line
        dss.Text.Command(
            f"New Line.{prot_line} Bus1={mid_bus} Bus2=bus{b2} "
            f"Phases=3 R1={r1} X1={x1} C1=0.0"
        )

        breaker_map[cb_name] = {
            "upstream_bus": bus_up,
            "mid_bus": mid_bus,
            "downstream_bus": f"bus{b2}",
            "protected_line": f"Line.{prot_line}",
            "line_index": index_counter,
        }

        index_counter += 1

    # -------------------------------------------------
    # 2️⃣ GRID-TIES (Activated + Service Restoration)
    # -------------------------------------------------
    for tie_cb_name, (bus1, bus2, r, x) in all_grid_ties:

        cb_name = f"CB{index_counter}"
        mid_bus = f"{bus1}_CB"
        prot_line = f"L_CB{index_counter}"

        # CB switch
        dss.Text.Command(
            f"New Line.{cb_name} Bus1={bus1} Bus2={mid_bus} "
            f"Phases=3 R1=1e-6 X1=1e-7 Switch=True"
        )

        # Protected tie line
        dss.Text.Command(
            f"New Line.{prot_line} Bus1={mid_bus} Bus2={bus2} "
            f"Phases=3 R1={r} X1={x} C1=0.0"
        )

        breaker_map[cb_name] = {
            "upstream_bus": bus1,
            "mid_bus": mid_bus,
            "downstream_bus": bus2,
            "protected_line": f"Line.{prot_line}",
            "line_index": index_counter,
        }

        index_counter += 1

    dss.Solution.Solve()
    logging.info(f"✅ Installed {len(breaker_map)} breakers total (feeders + grid-ties).")

    return breaker_map


def sync_cb_states(breaker_map):
    """
    Load existing CB states if available, otherwise initialize to 'ON'.
    """
    # Load old state
    if os.path.exists(CB_STATE_PATH):
        try:
            with open(CB_STATE_PATH, "r") as f:
                old_states = json.load(f)
        except:
            old_states = {}
    else:
        old_states = {}

    # Merge states
    for name, info in breaker_map.items():
        info["status"] = old_states.get(name, {}).get("status", "ON")

    # Save
    with open(CB_STATE_PATH, "w") as f:
        json.dump(breaker_map, f, indent=2)

    return breaker_map








# Fault control globals
FAULT_SCHEDULED = False
FAULT_INJECTED = False
FAULT_BUS = "bus15"


# ──────────────────────────────────────────────
# Base IEEE-33 Simulation + Export
# ──────────────────────────────────────────────
def build_and_export_ieee33():
    try:
        logging.info("🔄 Building IEEE-33 base system and exporting...")

        dss.Text.Command("Clear")
        dss.Basic.ClearAll()

        # --- Create Circuit ---
        dss.Text.Command("New Circuit.IEEE33 basekv=12.66 pu=1.0 phases=3 bus1=bus1")
        dss.Text.Command("Edit Vsource.Source bus1=bus1 phases=3 pu=1.0 basekv=12.66 angle=0")

        # --- Define Lines ---
        feeder_lines = [
            (1, 2, 0.0922, 0.0470), (2, 3, 0.4930, 0.2511), (3, 4, 0.3660, 0.1864),
            (4, 5, 0.3811, 0.1941), (5, 6, 0.8190, 0.7070), (6, 7, 0.1872, 0.6188),
            (7, 8, 1.7114, 1.2351), (8, 9, 1.0300, 0.7400), (9, 10, 1.0440, 0.7400),
            (10, 11, 0.1966, 0.0650), (11, 12, 0.3744, 0.1238), (12, 13, 1.4680, 1.1550),
            (13, 14, 0.5416, 0.7129), (14, 15, 0.5910, 0.5260), (15, 16, 0.7463, 0.5450),
            (16, 17, 1.2890, 1.7210), (17, 18, 0.7320, 0.5740), (2, 19, 0.1640, 0.1565),
            (19, 20, 1.5042, 1.3554), (20, 21, 0.4095, 0.4784), (21, 22, 0.7089, 0.9373),
            (3, 23, 0.4512, 0.3083), (23, 24, 0.8980, 0.7091), (24, 25, 0.8960, 0.7011),
            (6, 26, 0.2030, 0.1034), (26, 27, 0.2842, 0.1447), (27, 28, 1.0590, 0.9337),
            (28, 29, 0.8042, 0.7006), (29, 30, 0.5075, 0.2585), (30, 31, 0.9744, 0.9630),
            (31, 32, 0.3105, 0.3619), (32, 33, 0.3410, 0.5302),
        ]
        for idx, (b1, b2, r, x) in enumerate(feeder_lines, start=1):
            dss.Text.Command(f"New Line.L{idx} Bus1=bus{b1} Bus2=bus{b2} Phases=3 R1={r} X1={x} C1=0.0")



        # --- Load Data ---
        sample_loads = [
            (0, 0), (100, 60), (90, 40), (120, 80), (60, 30), (60, 20),
            (200, 100), (200, 100), (60, 20), (60, 20), (45, 30), (60, 35),
            (60, 35), (120, 80), (60, 10), (60, 20), (60, 20), (90, 40),
            (90, 40), (90, 40), (90, 40), (90, 40), (90, 50), (420, 200),
            (420, 200), (60, 25), (60, 25), (60, 20), (120, 70), (200, 600),
            (150, 70), (210, 100), (60, 40)
        ]


        load_multiplier = 1
        dss.Text.Command(f"Set LoadMult={load_multiplier}")
        logging.info(f"⚙️ Load Multiplier = {load_multiplier}")

        # Create loads
        for bus_idx, (kW, kvar) in enumerate(sample_loads, start=1):
            if kW == 0 and kvar == 0:
                continue
            dss.Text.Command(
                f"New Load.L{bus_idx} Bus1=bus{bus_idx}.1.2.3 "
                f"Phases=3 Conn=wye Model=1 kW={kW} kvar={kvar} kv=12.66"
            )
            
        # --- Initialize GridSystem ---
        system = GridSystem()
        for bus_idx, (kW, kvar) in enumerate(sample_loads, start=1):
            if kW == 0 and kvar == 0:
                continue
            system.loads.append({
                "bus": f"bus{bus_idx}",
                "kW": kW,
                "kvar": kvar,
                "load_multiplier": load_multiplier
            })


        # --- Fixed Capacitors (from original code) ---
        # Load fixed capacitors from JSON
        with open(COMPENSATOR_DEVICE_PATH, "r") as file:
            compensator_data = json.load(file)
        fixed_capacitors_data = compensator_data.get("fixed_capacitors", {})

        # Build the capacitors list from JSON
        capacitors = [
            (f"Cap{bus[3:]}", bus, kvar, 12.66)
            for bus, kvar in fixed_capacitors_data.items()
        ]

        for name, bus, kvar, kv in capacitors:
            # create with their original names so existing workflow remains
            try:
                dss.Text.Command(f"New Capacitor.{name} Bus1={bus} Phases=3 kVAR={kvar} kV={kv}")
            except Exception:
                # if already exists, edit
                try:
                    dss.Text.Command(f"Edit Capacitor.{name} kVAR={kvar} enabled=yes")
                except Exception:
                    pass
            system.capacitors.append({
                "name": name,
                "bus": bus,
                "kVAR": kvar,
                "kV": kv,
                "phases": 3
            })

        #------------------Service-Restoration Grid-Ties---------------------------
        # =========================================================================
        if os.path.exists(SR_FILE):
            try:
                with open(SR_FILE, "r") as f:
                    SR_activated_devices = json.load(f)
                logging.info(f"🔌 Loaded SR_activated devices: {SR_activated_devices}")

                # Load device parameter data
                comp_path = "/app/data/compensator_device.json"
                if not os.path.exists(comp_path):
                    logging.error(f"❌ compensator_device.json not found at {comp_path}")
                    comp_data = {}
                else:
                    with open(comp_path, "r") as f:
                        comp_data = json.load(f)
                    logging.info("🧭 Loaded SR compensator device parameters.")

                # ---- Activate grid-tie CBs (no mid-bus) ----
                for grid_name in SR_activated_devices.get("grid_ties", []):
                    try:
                        if grid_name in comp_data.get("tie_impedance", {}):
                            r, x = comp_data["tie_impedance"][grid_name]
                            bus1, bus2 = grid_name.split("-")
                            cb_name = f"{grid_name}_CB"

                            dss.Text.Command(
                                f"New Line.{cb_name} Bus1={bus1}.1.2.3 Bus2={bus2}.1.2.3 "
                                f"Phases=3 R1={r} X1={x} C1=0 Enabled=Yes"
                            )

                            logging.info(f"🔌 Grid-tie CB created: {cb_name} ({bus1} ↔ {bus2}, R={r}, X={x})")
                        else:
                            logging.warning(f"⚠️ No impedance data found for {grid_name}, skipping CB creation.")
                    except Exception as e:
                        logging.error(f"❌ Could not activate grid-tie CB {grid_name}: {e}")

            except json.JSONDecodeError:
                logging.error(f"❌ Invalid JSON format in {SR_FILE}. Skipping device activation.")
            except Exception as e:
                logging.error(f"❌ Failed to apply activated devices: {e}")
        else:
            logging.info(f"ℹ️ No SR_activated_devices.json found, skipping activation.")


        # ---- Service Restoration activated grid-ties for CB sync ----
        activated_ties_SR = []
        act_file = "/shared_volume/SR_activated_grid_tie.json"

        if os.path.exists(act_file):
            with open(act_file, "r") as f:
                data = json.load(f)
                for tie in data.get("grid_ties", []):
                    bus1, bus2 = tie.split("-")
                    r, x = comp_data["tie_impedance"][tie]
                    activated_ties_SR.append((f"{tie}_CB", (bus1, bus2, r, x)))  # << CB NAME HERE

        # =========================================================================




        # --- Include Activated Devices (use compensator_device.json for params) ---
        # ================= Include Activated Devices =================
        if os.path.exists(ACTIVATED_DEVICES_PATH):
            try:
                with open(ACTIVATED_DEVICES_PATH, "r") as f:
                    activated_devices = json.load(f)
                logging.info(f"🔌 Loaded activated devices: {activated_devices}")

                # Load device parameter data
                comp_path = "/app/data/compensator_device.json"
                if not os.path.exists(comp_path):
                    logging.error(f"❌ compensator_device.json not found at {comp_path}")
                    comp_data = {}
                else:
                    with open(comp_path, "r") as f:
                        comp_data = json.load(f)
                    logging.info("🧭 Loaded compensator device parameters.")

                # ---- Activate capacitors ----
                for cap_name in activated_devices.get("capacitors", []):
                    try:
                        kvar_val = comp_data.get("capacitor_reactive_power", {}).get(cap_name)
                        if kvar_val is None:
                            kvar_val = comp_data.get("fixed_capacitors", {}).get(cap_name, 100)  # fallback
                            logging.warning(f"⚠️ No exact kVAR found for {cap_name}, using {kvar_val} kvar default")

                        dss.Text.Command(
                            f"New Capacitor.{cap_name} Bus1={cap_name} Phases=3 kVAR={kvar_val} kV=12.66"
                        )
                        logging.info(f"✅ Capacitor {cap_name} activated with {kvar_val} kVAR.")
                    except Exception as e:
                        logging.warning(f"⚠️ Could not activate capacitor {cap_name}: {e}")

                # ---- Activate reactors ----
                for reac_name in activated_devices.get("reactors", []):
                    try:
                        kvar_val = comp_data.get("shunt_reactor_reactive_power", {}).get(reac_name, 500)
                        dss.Text.Command(
                            f"New Reactor.{reac_name} Bus1={reac_name} Phases=3 kVAR={kvar_val} kV=12.66"
                        )
                        logging.info(f"✅ Reactor {reac_name} activated with {kvar_val} kVAR.")
                    except Exception as e:
                        logging.warning(f"⚠️ Could not activate reactor {reac_name}: {e}")

                # ---- Activate grid-tie CBs (no mid-bus) ----
                for grid_name in activated_devices.get("grid_ties", []):
                    try:
                        if grid_name in comp_data.get("tie_impedance", {}):
                            r, x = comp_data["tie_impedance"][grid_name]
                            bus1, bus2 = grid_name.split("-")
                            cb_name = f"{grid_name}_CB"

                            dss.Text.Command(
                                f"New Line.{cb_name} Bus1={bus1}.1.2.3 Bus2={bus2}.1.2.3 "
                                f"Phases=3 R1={r} X1={x} C1=0 Enabled=Yes"
                            )

                            logging.info(f"🔌 Grid-tie CB created: {cb_name} ({bus1} ↔ {bus2}, R={r}, X={x})")
                        else:
                            logging.warning(f"⚠️ No impedance data found for {grid_name}, skipping CB creation.")
                    except Exception as e:
                        logging.error(f"❌ Could not activate grid-tie CB {grid_name}: {e}")

            except json.JSONDecodeError:
                logging.error(f"❌ Invalid JSON format in {ACTIVATED_DEVICES_PATH}. Skipping device activation.")
            except Exception as e:
                logging.error(f"❌ Failed to apply activated devices: {e}")
        else:
            logging.info(f"ℹ️ No activated_devices.json found, skipping activation.")


        # ---- Load activated grid-ties for CB sync ----
        activated_ties = []
        act_file = "/shared_volume/activated_devices.json"

        if os.path.exists(act_file):
            with open(act_file, "r") as f:
                data = json.load(f)
                for tie in data.get("grid_ties", []):
                    bus1, bus2 = tie.split("-")
                    r, x = comp_data["tie_impedance"][tie]
                    activated_ties.append((f"{tie}_CB", (bus1, bus2, r, x)))  # << CB NAME HERE


        # ---- Apply CB states ----
        breaker_map = install_circuit_breakers(
            feeder_lines,
            activated_ties=activated_ties,
            activated_ties_SR=activated_ties_SR
        )

        breaker_map = sync_cb_states(breaker_map)

        for cb_name, info in breaker_map.items():
                    status = info.get("status", "ON").upper()
                    try:
                        dss.Circuit.SetActiveElement(f"Line.{cb_name}")
                        if status == "OFF":
                            # Disable (open) the breaker line
                            dss.CktElement.Enabled(False)
                            logging.info(f"🔴 Breaker {cb_name} set to OFF (opened) in OpenDSS.")
                        else:
                            # Enable (close) the breaker line
                            dss.CktElement.Enabled(True)
                            logging.info(f"🟢 Breaker {cb_name} set to ON (closed) in OpenDSS.")
                    except Exception as e:
                        logging.warning(f"⚠️ Could not apply {status} to {cb_name}: {e}")




        # --- Inject Fault (if scheduled) ---
        # --- Fault persistence logic ---
        global FAULT_SCHEDULED, FAULT_INJECTED, FAULT_BUS

        if FAULT_INJECTED:
            # If fault was already injected before, recreate it after rebuild
            try:
                logging.info(f"💥 Restoring persistent fault at {FAULT_BUS} after rebuild...")
                dss.Text.Command(f"New Fault.F1 Bus1={FAULT_BUS} Phases=3 r=0.001")
                logging.info(f"✅ Persistent fault re-established at {FAULT_BUS}.")
            except Exception as e:
                logging.error(f"❌ Failed to restore persistent fault: {e}")

        elif FAULT_SCHEDULED and not FAULT_INJECTED:
            # First-time fault injection
            try:
                logging.info(f"💥 Injecting new fault at {FAULT_BUS}...")
                dss.Text.Command(f"New Fault.F1 Bus1={FAULT_BUS} Phases=3 r=0.001")
                FAULT_INJECTED = True
                logging.info(f"✅ Fault successfully injected at {FAULT_BUS} (persistent).")
            except Exception as e:
                logging.error(f"❌ Fault injection failed: {e}")


        # --- Solve ---
        dss.Text.Command("Set Voltagebases=[12.66]")
        dss.Text.Command("CalcVoltageBases")
        dss.Text.Command("Solve")

        if not dss.Solution.Converged():
            logging.warning("⚠️ Power flow did not converge. Retrying...")
            dss.Text.Command("Solve Mode=Snap MaxControlIterations=20")

        # 🔍 Log all bus voltages after successful solve
        log_bus_voltages("Post-Solve Bus Voltages")
        cb_currents = log_cb_currents(breaker_map, title="Breaker Currents")

        # --- Collect Node and Branch Data ---
        Sbase_MVA = 100.0
        for bus in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus)
            volts = dss.Bus.Voltages()
            kv_base = dss.Bus.kVBase()
            if kv_base == 0 or not volts:
                continue
            v_real, v_imag = volts[0], volts[1]
            base_vll = kv_base * 1000
            pu_voltage = (v_real + 1j * v_imag) / base_vll

            p_kw, q_kvar = 0, 0
            for load_name in dss.Loads.AllNames():
                dss.Loads.Name(load_name)
                bus_connected = dss.CktElement.BusNames()[0].split('.')[0]
                if bus_connected.lower() == bus.lower():
                    p_kw += dss.Loads.kW()
                    q_kvar += dss.Loads.kvar()

            s_base_va = Sbase_MVA * 1e6
            power_va = complex(p_kw, q_kvar) * 1000
            power_pu = power_va / s_base_va

            system.nodes.append(Node(
                name=bus,
                uuid=str(uuid.uuid4()),
                voltage_pu=pu_voltage,
                power_pu=power_pu,
                base_voltage=kv_base * 1000,
                base_apparent_power=s_base_va,
                real_power=power_va.real,
                imag_power=power_va.imag,
                voltage_real=v_real,
                voltage_imag=v_imag,
                load_multiplier=load_multiplier
            ))

        for line_name in dss.Lines.AllNames():
            dss.Lines.Name(line_name)
            buses = dss.CktElement.BusNames()
            if not buses or len(buses) < 2:
                continue
            from_bus, to_bus = buses[0].split('.')[0], buses[1].split('.')[0]
            r, x, bch, length = dss.Lines.R1(), dss.Lines.X1(), dss.Lines.C1(), dss.Lines.Length()
            z = complex(r, x)
            z_base = (12.66 ** 2) / (Sbase_MVA * 1e6)
            z_pu = z / z_base

            system.branches.append(Branch(
                uuid=str(uuid.uuid4()),
                start_node=next((n for n in system.nodes if n.name == from_bus), None),
                end_node=next((n for n in system.nodes if n.name == to_bus), None),
                r=r, x=x, bch=bch, bch_pu=bch * length,
                length=length,
                base_voltage=12.66,
                base_apparent_power=Sbase_MVA * 1e6,
                r_pu=r / z_base,
                x_pu=x / z_base,
                z=z,
                z_pu=z_pu,
                type_="line"
            ))
      
        export_grid_to_excel(system, breaker_map=breaker_map, cb_currents=cb_currents, path="/shared_volume/grid_data1.xlsx")
        logging.info("✅ IEEE-33 base system exported successfully.")

        return True

    except Exception as e:
        logging.error(f"❌ Error building/exporting IEEE-33: {e}")
        return False


# ──────────────────────────────────────────────
# Periodic Power Flow (every 5 sec)
# ──────────────────────────────────────────────
def periodic_powerflow(interval_sec=10):
    """Rebuilds and exports IEEE-33 network every few seconds."""
    while True:
        try:
            logging.info(f"🔁 Running periodic IEEE-33 power flow and Excel export...")
            build_and_export_ieee33()
            logging.info(f"✅ Power flow cycle completed. Sleeping for {interval_sec} sec...")
        except Exception as e:
            logging.error(f"❌ Error in periodic power flow loop: {e}")
        time.sleep(interval_sec)

def schedule_fault_after_delay(delay_sec=180):
    """Set FAULT_SCHEDULED=True after given delay."""
    global FAULT_SCHEDULED
    time.sleep(delay_sec)
    FAULT_SCHEDULED = True
    logging.info(f"💥 Fault scheduled at {FAULT_BUS} after {delay_sec} seconds.")


if __name__ == "__main__":
    # Start periodic power flow in a background thread
    threading.Thread(target=periodic_powerflow, daemon=True).start()

    # Schedule fault injection after 5 minutes (120 sec)
    threading.Thread(target=schedule_fault_after_delay, args=(180,), daemon=True).start()

    # Keep Flask or main loop running
    app.run(host="0.0.0.0", port=4006)

