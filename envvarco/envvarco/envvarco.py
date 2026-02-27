import logging
import threading
import opendssdirect as dss
import numpy as np
from openpyxl import Workbook
import requests
from openpyxl.styles import Font
import pandas as pd
import json
import os
import re
import sys
import time
from datetime import datetime
from flask import Flask, request, jsonify
from src.oma_algorithm import (
    create_circuit,
    initialize_devices,
    combined_cap_reac_objective_opendss,
    fungal_growth_optimizer
)

app = Flask(__name__)
logging.basicConfig(filename="envarco.log", level=logging.INFO)

EXCEL_PATH = "/shared_volume/grid_data1.xlsx"
ACTIVATED_DEVICES_PATH = "/shared_volume/activated_devices.json"
CB_STATE_PATH = "/shared_volume/cb_states.json"
SR_FILE = "/shared_volume/SR_activated_grid_tie.json"
org_comp_path = "/app/data/compensator_device.json"
comp_path = "/app/data/new_compensator_device.json"

def print_voltage_table(title):
    logging.info(f"\n=== {title.upper()} ===")
    logging.info(f"{'Bus':<8}{'Voltage (p.u.)':>15}")
    logging.info("-" * 25)
    for bus in dss.Circuit.AllBusNames():
        dss.Circuit.SetActiveBus(bus)
        vmag = dss.Bus.puVmagAngle()[0]
        logging.info(f"{bus:<8}{vmag:>15.4f}")

MAX_RETRIES = 3
RETRY_DELAY_SEC = 2

def remove_sr_grid_ties(comp_file=org_comp_path,
                        sr_file=SR_FILE,
                        output_file=comp_path):
    """
    Creates new_compensator_device.json with the following rule:

    • If SR_activated_grid_tie.json DOES NOT exist:
        → Copy compensator_device.json unchanged.

    • If SR_activated_grid_tie.json EXISTS:
        → Copy full compensator_device.json BUT remove ONLY the grid tie
          listed in SR_activated_grid_tie.json and its impedance.
    """

    # ---- Load compensator_device.json ----
    with open(comp_file, "r") as f:
        comp = json.load(f)

    # ---- If SR_activated_grid_tie.json NOT found → copy whole file ----
    if not os.path.exists(sr_file):
        print("⚠ SR file not found → copying compensator_device.json unchanged.")
        with open(output_file, "w") as f:
            json.dump(comp, f, indent=4)
        return

    # ---- Load SR file ----
    with open(sr_file, "r") as f:
        sr = json.load(f)

    sr_ties = sr.get("grid_ties", [])

    # ---- If SR file exists but contains NO ties → copy whole file ----
    if not sr_ties:
        print("⚠ SR file contains no grid ties → copying unchanged.")
        with open(output_file, "w") as f:
            json.dump(comp, f, indent=4)
        return

    # ----------------------------------------------------------
    # Prepare list of ties to remove (e.g., "bus18-bus33")
    # ----------------------------------------------------------
    sr_tie_pairs = []
    for tie in sr_ties:
        b1, b2 = tie.split("-")
        sr_tie_pairs.append((b1, b2))
        sr_tie_pairs.append((b2, b1))  # also remove reversed order

    # ----------------------------------------------------------
    # Remove ONLY those ties from tie_switches
    # ----------------------------------------------------------
    new_switches = []
    for pair in comp.get("tie_switches", []):
        if tuple(pair) not in sr_tie_pairs:
            new_switches.append(pair)

    comp["tie_switches"] = new_switches

    # ----------------------------------------------------------
    # Remove ONLY those impedance entries
    # ----------------------------------------------------------
    new_imp = {}
    for key, val in comp.get("tie_impedance", {}).items():
        b1, b2 = key.split("-")
        if (b1, b2) not in sr_tie_pairs:
            new_imp[key] = val

    comp["tie_impedance"] = new_imp

    # ----------------------------------------------------------
    # Write new file
    # ----------------------------------------------------------
    with open(output_file, "w") as f:
        json.dump(comp, f, indent=4)

    print(f"✅ {output_file} created successfully (SR grid ties removed).")

def load_excel_with_retry(path, max_retries=MAX_RETRIES, delay=RETRY_DELAY_SEC):
    for attempt in range(1, max_retries + 1):
        try:
            return pd.ExcelFile(path)
        except Exception as e:
            logging.warning(f"⚠️ Attempt {attempt} failed to load Excel: {e}")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                logging.error(f"❌ Failed to load Excel after {max_retries} attempts.")
                raise

def install_circuit_breakers(feeder_lines, grid_ties=None):
    """
    Install CBs for both feeder lines and grid-tie lines.
    Uses the SAME naming convention: busX -> busX_CB -> destination.
    """
    logging.info("\n🔌 Installing circuit breakers for feeders and grid-ties...")
    breaker_map = {}

    if grid_ties is None:
        grid_ties = []

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

        # protected feeder line
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
    # 2️⃣ GRID-TIE LINES  (same CB notation)
    # -------------------------------------------------
    for tie_name, (bus1, bus2, r, x) in grid_ties:

        # Example: tie "bus12-bus22"
        cb_name = tie_name.replace("-", "_")        # bus12_bus22
        mid_bus = f"{bus1}_CB"                      # SAME as feeder CB notation
        prot_line = f"{cb_name}_line"

        # CB switch
        dss.Text.Command(
            f"New Line.{cb_name} Bus1={bus1} Bus2={mid_bus} "
            f"Phases=3 R1=1e-6 X1=1e-7 Switch=True"
        )

        # protected tie line
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
    logging.info(f"✅ Installed {len(breaker_map)} breakers (feeders + grid-ties).")

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

def get_deenergized_buses_from_cb(breaker_map, v_zero_thresh=0.05):
    """
    Returns a set of buses that are de-energized due to OFF CBs.
    """
    deenergized_buses = set()

    for cb_name, info in breaker_map.items():
        if info.get("status", "ON").upper() == "OFF":
            # downstream bus of OFF CB
            down_bus = info.get("downstream_bus")
            if down_bus:
                deenergized_buses.add(down_bus)

    return deenergized_buses

def extract_feeder_lines_from_excel(path):
    df = pd.read_excel(path, sheet_name="branches")

    # regex: matches bus1, bus2, ..., bus33
    bus_pattern = re.compile(r"^bus(\d+)$")

    feeder_lines = []

    print("\n📌 Extracting feeder lines (ignoring CB segments)...\n")

    for _, row in df.iterrows():
        from_bus = str(row['from']).strip()
        to_bus = str(row['to']).strip()

        # Match only "bus<number>" — no '_cb'
        m1 = bus_pattern.match(from_bus)
        m2 = bus_pattern.match(to_bus)

        if not (m1 and m2):
            continue

        # Extract integer bus numbers
        bus1 = int(m1.group(1))
        bus2 = int(m2.group(1))
        r = float(row['r'])
        x = float(row['x'])

        feeder_lines.append((bus1, bus2, r, x))

        # Print each line
        print(f"✔ Feeder: bus{bus1} → bus{bus2}  |  R={r}  X={x}")

    print("\n✅ Total feeder lines extracted:", len(feeder_lines))
    print("➡ Final Feeder Line List:")
    for line in feeder_lines:
        print(line)

    return feeder_lines

def build_ieee33_system_from_excel():
    try:
        if not os.path.exists(EXCEL_PATH):
            logging.error(f"❌ Excel file not found: {EXCEL_PATH}")
            return

        xls = load_excel_with_retry(EXCEL_PATH)
        dss.Text.Command("Clear")
        dss.Basic.ClearAll()
        dss.Text.Command("New Circuit.RebuiltIEEE33 basekv=12.66 pu=1.0 phases=3 bus1=bus1")
        dss.Text.Command("Edit Vsource.Source bus1=bus1 phases=3 pu=1.0 basekv=12.66 angle=0")

        # --- Define Lines ---
        feeder_lines = extract_feeder_lines_from_excel(EXCEL_PATH)

        for idx, (b1, b2, r, x) in enumerate(feeder_lines, start=1):
            dss.Text.Command(f"New Line.L{idx} Bus1=bus{b1} Bus2=bus{b2} Phases=3 R1={r} X1={x} C1=0.0")


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

        #------------------Service-Restoration Grid-Ties---------------------------
        # =========================================================================
        if os.path.exists(SR_FILE):
            try:
                with open(SR_FILE, "r") as f:
                    SR_activated_devices = json.load(f)
                logging.info(f"🔌 Loaded SR_activated devices: {SR_activated_devices}")

                # Load device parameter data
                #comp_path = "/app/data/compensator_device.json"
                if not os.path.exists(org_comp_path):
                    logging.error(f"❌ compensator_device.json not found at {org_comp_path}")
                    comp_data = {}
                else:
                    with open(org_comp_path, "r") as f:
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
        breaker_map = install_circuit_breakers(feeder_lines, activated_ties)
        breaker_map = sync_cb_states(breaker_map)

        for cb_name, info in breaker_map.items():
            status = info.get("status", "ON").upper()
            try:
                dss.Circuit.SetActiveElement(f"Line.{cb_name}")
                dss.CktElement.Enabled(status == "ON")
                logging.info(f"CB {cb_name} → {status}")
            except Exception as e:
                logging.warning(f"⚠️ Could not apply {status} to {cb_name}: {e}")



        # ================= Power Flow Solve =================
        dss.Text.Command("Set Voltagebases=[12.66]")
        dss.Text.Command("CalcVoltageBases")
        dss.Text.Command("Solve")
        if not dss.Solution.Converged():
            logging.warning("⚠️ Power flow did not converge, retrying...")
            dss.Text.Command("Solve Mode=Snap MaxControlIterations=20")

        logging.info(f"✅ Circuit rebuilt at {datetime.now().strftime('%H:%M:%S')}")
        return breaker_map

    except Exception as e:
        logging.exception(f"❌ Error rebuilding circuit: {e}")
        return None

 
def get_activated_devices(solution, capacitor_buses, reactor_buses, tie_switches):
    cap_keys = list(capacitor_buses.keys())
    reac_keys = list(reactor_buses.keys())
    tie_keys = [f"{bus1}-{bus2}" for bus1, bus2 in tie_switches]

    activated_caps = [bus for i, bus in enumerate(cap_keys) if solution[i] >= 0.5]
    activated_reacs = [bus for j, bus in enumerate(reac_keys) if solution[len(cap_keys) + j] >= 0.5]
    activated_ties = [
        tie for k, tie in enumerate(tie_keys)
        if solution[len(cap_keys) + len(reac_keys) + k] >= 0.5
    ]
    return activated_caps, activated_reacs, activated_ties

def apply_solution_to_circuit(solution, capacitor_buses, reactor_buses, tie_switches):
    """
    Apply provided binary/continuous solution vector to the OpenDSS circuit.
    (Useful for temporary apply when measuring voltages)
    """
    logging.info(f"✅ Applying solution to circuit (temporary): {solution}")

    # Reset all devices to disabled
    for bus in capacitor_buses:
        dss.Text.Command(f"Edit Capacitor.Cap_{bus} enabled=no")
    for bus in reactor_buses:
        dss.Text.Command(f"Edit Reactor.Reac_{bus} enabled=no")
    for bus1, bus2 in tie_switches:
        name = f"Tie_{bus1}_{bus2}"
        dss.Text.Command(f"Edit Line.{name} enabled=no")

    solution_bin = np.where(solution >= 0.5, 1, 0)
    cap_keys = list(capacitor_buses.keys())
    reac_keys = list(reactor_buses.keys())
    tie_keys = [f"{bus1}-{bus2}" for bus1, bus2 in tie_switches]

    for i, bus in enumerate(cap_keys):
        dss.Text.Command(f"Edit Capacitor.Cap_{bus} enabled={'yes' if solution_bin[i] else 'no'}")
    for j, bus in enumerate(reac_keys):
        idx = len(cap_keys) + j
        dss.Text.Command(f"Edit Reactor.Reac_{bus} enabled={'yes' if solution_bin[idx] else 'no'}")
    for k, tie in enumerate(tie_keys):
        idx = len(cap_keys) + len(reac_keys) + k
        bus1, bus2 = tie.split("-")
        name = f"Tie_{bus1}_{bus2}"
        dss.Text.Command(f"Edit Line.{name} enabled={'yes' if solution_bin[idx] else 'no'}")

    dss.Text.Command("Solve Mode=Snap")
    logging.info("🔧 Solution applied (temporary) and power flow solved.")

def load_activated_devices():
    try:
        with open(ACTIVATED_DEVICES_PATH, "r") as f:
            data = json.load(f)
            return data
    except FileNotFoundError:
        logging.info("ℹ️ activated_devices.json not found — assuming none active.")
        return {}
    except Exception as e:
        logging.error(f"❌ Error reading activated devices file: {e}")
        return {}

def nullify_activated_devices(activated_data, capacitor_buses, reactor_buses, tie_switches):
    """
    Turn OFF devices that are recorded active in activated_data.
    This creates the 'nullified' circuit baseline for the optimizer.
    Returns lists [activated_caps, activated_reacs, activated_ties] that were turned off.
    """
    activated_caps = []
    activated_reacs = []
    activated_ties = []

    # Capacitors
    caps = activated_data.get("capacitor_reactive_power", {}) or {}
    for bus in caps.keys():
        try:
            dss.Text.Command(f"Edit Capacitor.Cap_{bus} enabled=no")
        except Exception:
            pass
        try:
            dss.Text.Command(f"Edit Capacitor.{bus} enabled=no")
        except Exception:
            pass
        activated_caps.append(bus)

    # Reactors
    reacs = activated_data.get("shunt_reactor_reactive_power", {}) or {}
    for bus in reacs.keys():
        try:
            dss.Text.Command(f"Edit Reactor.Reac_{bus} enabled=no")
        except Exception:
            pass
        try:
            dss.Text.Command(f"Edit Reactor.{bus} enabled=no")
        except Exception:
            pass
        activated_reacs.append(bus)

    # Tie switches
    ties = activated_data.get("tie_switches", []) or []
    for pair in ties:
        if isinstance(pair, list) and len(pair) == 2:
            bus1, bus2 = pair
        elif isinstance(pair, str) and "-" in pair:
            bus1, bus2 = pair.split("-")
        else:
            continue
        name = f"Tie_{bus1}_{bus2}"
        try:
            dss.Text.Command(f"Edit Line.{name} enabled=no")
        except Exception:
            pass
        activated_ties.append(f"{bus1}-{bus2}")

    # Solve to produce baseline
    dss.Text.Command("Solve Mode=Snap")
    logging.info("🧾 Nullified recorded devices and solved baseline circuit.")
    return activated_caps, activated_reacs, activated_ties

def optimize():
    try:
        start = 1
        logging.info("📡 Optimization started.")

        # 1) Build base circuit from Excel
        build_ieee33_system_from_excel()
     
        print_voltage_table("Bus Voltages BEFORE any nullify/optimization")

        # 2) Load device config
        device_config = {}
        try:
            with open("/app/data/compensator_device.json", "r") as f:
                device_config = json.load(f)
                logging.info("📄 Loaded device config for ease-of-influence scoring.")
        except Exception as e:
            logging.error(f"❌ Failed to load compensator_device.json: {e}")
            device_config = {}

        capacitor_buses = device_config.get("capacitor_reactive_power", {})
        reactor_buses = device_config.get("shunt_reactor_reactive_power", {})
        tie_switches = device_config.get("tie_switches", [])
        tie_impedance = device_config.get("tie_impedance", {})

        # 3) Initialize device definitions (create devices disabled by default)
        initialize_devices(capacitor_buses, reactor_buses, tie_switches, tie_impedance)
        dim = len(capacitor_buses) + len(reactor_buses) + len(tie_switches)

        # 4) Load activated devices (previous run) and nullify their effect
        activated_data = load_activated_devices()
        activated_caps, activated_reacs, activated_ties = nullify_activated_devices(
            activated_data, capacitor_buses, reactor_buses, tie_switches
        )

        # (Optional) log baseline voltages
        logging.info("Baseline voltages after nullify:")
        for bus in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus)
            v = dss.Bus.puVmagAngle()[0]
            logging.info(f"  {bus}: {v:.4f}")

        # 5) Run optimizer on the nullified circuit
        pareto_front, best_solution = fungal_growth_optimizer(
            N=100, Tmax=500, dim=dim,
            fobj=lambda sol: combined_cap_reac_objective_opendss(
                sol, capacitor_buses, reactor_buses, tie_switches, tie_impedance
            )
        )

        logging.info(f"🏆 Best solution found by optimizer (on nullified circuit): {best_solution}")

        # 6) Temporarily apply the optimizer solution to capture post-optimization voltages
        apply_solution_to_circuit(best_solution, capacitor_buses, reactor_buses, tie_switches)

        # --- Build Excel workbook (Voltage Optimization Summary) using applied solution voltages ---
        wb = Workbook()
        ws = wb.active
        ws.title = "Voltage Optimization Summary"
        ws.append(["Node", "Device Type", "Reactive Power (MVAR)", "Voltage Status", "Tap Changed", "No. of Tap Adjustments"])
        bold_font = Font(bold=True)
        for cell in ws[1]:
            cell.font = bold_font

        # get lists of activated devices according to best_solution
        new_caps, new_reacs, new_ties = get_activated_devices(best_solution, capacitor_buses, reactor_buses, tie_switches)
        logging.info(f"New activated caps: {new_caps}, reacs: {new_reacs}, ties: {new_ties}")

        device_map = {}
        for bus in new_caps:
            q_mvar = capacitor_buses.get(bus, 0)
            device_map.setdefault(bus, []).append(("Capacitor", q_mvar))
        for bus in new_reacs:
            q_mvar = reactor_buses.get(bus, 0)
            device_map.setdefault(bus, []).append(("Reactor", q_mvar))

        # collect OLTC info later (we keep tap map empty here because trigger does physical changes)
        tap_adjustment_map = {}

        for bus in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus)
            vmag = dss.Bus.puVmagAngle()[0:6:2]
            v_pu = vmag[0] if vmag else 1.0
            if v_pu > 1.05:
                v_status = "Overvoltage"
            elif v_pu < 0.95:
                v_status = "Undervoltage"
            else:
                v_status = "Normal"
            tap_count = tap_adjustment_map.get(bus, 0)
            tap_changed = "Yes" if tap_count != 0 else "No"

            if bus in device_map:
                for dev_type, q_mvar in device_map[bus]:
                    ws.append([bus, dev_type, q_mvar, v_status, tap_changed, tap_count])
            else:
                ws.append([bus, "", "", v_status, tap_changed, tap_count])

        for tie in new_ties:
            bus1, bus2 = tie.split("-")
            r, x = tie_impedance.get(tie, [0.0, 0.0])
            tie_label = f"{bus1}-{bus2}"
            impedance_str = f"R={r}, X={x}"
            ws.append([tie_label, "Grid-Tie", impedance_str, "", "", ""])

        try:
            wb.save("/shared_volume/voltage_optimization_summary.xlsx")
            logging.info("✅ Voltage optimization summary exported successfully.")
        except Exception as e:
            logging.error(f"❌ Failed to export voltage summary: {e}")

        # 7) Revert circuit back to nullified state (turn OFF all devices again)
        # (so trigger.py is responsible for making actual device changes)
        zero_solution = np.zeros(dim)
        apply_solution_to_circuit(zero_solution, capacitor_buses, reactor_buses, tie_switches)
        logging.info("Reverted circuit to nullified state; trigger.py will perform actual switching.")

        # 8) Trigger the trigger service that will perform actual activation (trigger.py)
        try:
            response = requests.post("http://trigger_var_control:4004/optimize", json={"start": start})
            if response.status_code == 200:
                logging.info("✅ Trigger service acknowledged the optimization request.")
            else:
                logging.warning(f"⚠️ Trigger service returned: {response.status_code} - {response.text}")
        except Exception as e:
            logging.error(f"❌ Failed to call trigger service: {e}")

        return {"best_solution": best_solution.tolist() if hasattr(best_solution, "tolist") else best_solution}


    except Exception as e:
        logging.error(f"❌ Optimization error: {e}", exc_info=True)
        return {"error": str(e)}

def check_voltage_violations(breaker_map=None):
    """
    Returns:
      alive_voltages : list of (bus, voltage)
      violated       : True/False
    """

    deenergized_buses = set()
    if breaker_map:
        deenergized_buses = get_deenergized_buses_from_cb(breaker_map)

    alive_voltages = []
    violated = False

    for bus in dss.Circuit.AllBusNames():
        dss.Circuit.SetActiveBus(bus)
        vmag = dss.Bus.puVmagAngle()[0]

        # -------------------------------
        # 1️⃣ Ignore de-energized buses
        # -------------------------------
        if bus in deenergized_buses and vmag < 0.05:
            logging.info(f"⚠ Ignoring {bus}: CB OFF & V≈0 ({vmag:.3f} pu)")
            continue

        # -------------------------------
        # 2️⃣ Ignore numerically dead buses
        # -------------------------------
        if vmag < 0.05:
            logging.info(f"⚠ Ignoring {bus}: Electrically dead (V≈0)")
            continue

        alive_voltages.append((bus, vmag))

        # -------------------------------
        # 3️⃣ Voltage violation check
        # -------------------------------
        if vmag < 0.95 or vmag > 1.05:
            violated = True

    return alive_voltages, violated


# =====================================================
# BACKGROUND SCHEDULER (RUN EVERY 15 SECONDS)
# =====================================================
def periodic_optimize():
    """Continuously check CB line currents; if <250A then run full logic, else skip."""
    while True:
        try:
            remove_sr_grid_ties()
            # ---------------------------------------------------------
            # 1) READ grid_data1.xlsx → CB sheet → line_current_A column
            # ---------------------------------------------------------
            try:
                xls = load_excel_with_retry(EXCEL_PATH)
                cb_df = pd.read_excel(xls, sheet_name="CB")
            except Exception as e:
                logging.error(f"❌ Failed to read CB sheet from Excel: {e}")
                time.sleep(15)
                continue

            # Ensure column exists
            if "line_current_A" not in cb_df.columns:
                logging.error("❌ 'line_current_A' column not found in CB sheet.")
                time.sleep(15)
                continue

            # Check if ALL line currents are below 250A
            all_below_threshold = (cb_df["line_current_A"] < 250).all()

            if all_below_threshold:
                print("🔍 All CB currents < 250A → Proceeding with voltage checks & optimization logic")
                logging.info("All CB currents < 250A → Proceeding with full optimization logic")

                # ---------------------------------------------------------
                # 2) Build circuit and check voltages
                # ---------------------------------------------------------
                print("⚡ Building IEEE-33 circuit from Excel...")
                breaker_map = build_ieee33_system_from_excel()

                if breaker_map is None:
                    logging.error("❌ breaker_map unavailable — skipping voltage check")
                    time.sleep(15)
                    continue


                print("📊 Checking bus voltages BEFORE any optimization...")
                print_voltage_table("Bus Voltages BEFORE any nullify/optimization")

                # Get list of bus voltages
                alive_voltages, violated = check_voltage_violations(breaker_map)

                if violated:
                    print("⚠ Alive bus voltage violation detected → Running optimize()")
                    logging.warning(f"Voltage violation on alive buses: {alive_voltages}")
                    optimize()
                else:
                    print("✅ All alive buses within 0.95–1.05 pu → Skipping optimization")

            else:
                print("❌ One or more CB currents ≥ 250A → Skipping optimization cycle")
                logging.warning("CB current ≥ 250A detected → Skipping this cycle")

        except Exception as e:
            logging.error(f"❌ Error in periodic optimize loop: {e}", exc_info=True)

        time.sleep(15)


# =====================================================
# STARTUP
# =====================================================
if __name__ == "__main__":
    logging.info("🚀 Starting automatic optimizer service (15 sec interval)…")

    # Launch periodic optimizer in background
    scheduler_thread = threading.Thread(target=periodic_optimize, daemon=True)
    scheduler_thread.start()

    # Keep main thread alive indefinitely
    while True:
        time.sleep(3600)
