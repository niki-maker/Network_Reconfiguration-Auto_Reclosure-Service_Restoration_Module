# opendss33.py
import opendssdirect as dss
import math
from datetime import datetime
import pandas as pd
import numpy as np
import uuid
import time
from flask import Flask
import os
import logging
import networkx as nx
import json
import sys
# Add shared volume to Python path so we can import the base system
sys.path.append("/shared_volume_base")
from flask import Flask, jsonify, request

# Flask app
app = Flask(__name__)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
logger.addHandler(logging.FileHandler("bus_priority_score.log"))
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# 📁 Ensure shared volume folder exists
os.makedirs("/shared_volume", exist_ok=True)

EXCEL_PATH = "/shared_volume/grid_data1.xlsx"
ACTIVATED_DEVICES_PATH = "/shared_volume/activated_devices.json"

class Node:
    def __init__(self, name, uuid, voltage_pu, power_pu, base_voltage, base_apparent_power,
                 real_power, imag_power, voltage_real, voltage_imag):
        self.name = name
        self.uuid = uuid
        self.voltage_pu = voltage_pu  # complex
        self.power_pu = power_pu  # complex
        self.baseVoltage = base_voltage
        self.base_apparent_power = base_apparent_power
        self.power = complex(real_power, imag_power)
        self.voltage = complex(voltage_real, voltage_imag)
        # fields filled by priority routine
        self.r_imp = None
        self.r_sens = None
        self.load_score = None
        self.ease_score = None


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


def priority_score_exporter(system, path="/shared_volume/priority_score.xlsx"):
    """
    Export the final priority scores to an excel file (priority_scores sheet).
    """
    try:
        priority_records = []
        for node in system.nodes:
            uuid = node.uuid
            name = node.name

            # Recompute priority (if fields exist) to be robust
            r_imp = getattr(node, "r_imp", 1.0) or 1.0
            r_sens = getattr(node, "r_sens", 1.0) or 1.0
            r_elec = 0.6 * r_imp + 0.4 * r_sens

            load_score = getattr(node, "load_score", 0.0) or 0.0
            ease_score = getattr(node, "ease_score", 0.0) or 0.0

            priority_score = 0.4 * r_elec + 0.4 * load_score + 0.2 * ease_score

            priority_records.append({
                "bus_name": name,
                "uuid": uuid,
                "r_imp": round(r_imp, 6),
                "r_sens": round(r_sens, 6),
                "r_elec": round(r_elec, 6),
                "load_score": round(load_score, 6),
                "ease_score": round(ease_score, 6),
                "priority_score": round(priority_score, 6)
            })

        priority_df = pd.DataFrame(priority_records)

        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            priority_df.to_excel(writer, sheet_name="priority_scores", index=False)

        logging.info(f"📁 Priority scores exported to {path}")
        return True
    except Exception as e:
        logging.error(f"❌ Priority score export failed: {e}")
        return False


def compute_priority_scores(system, sbase_mva=100.0, injected_kvar=500.0):
    """
    Compute bus priority scores on the GridSystem object which was created from OpenDSS.
    - sbase_mva: used to normalize, kept for compatibility
    - injected_kvar: reactive injection per bus during sensitivity sweep (in kvar)
    """
    try:
        logging.info("🔎 Starting priority score computation...")

        # Build graph using branch z_pu magnitude as weight
        G = nx.Graph()
        for br in system.branches:
            if br.start_node and br.end_node:
                weight = abs(br.z_pu) if br.z_pu is not None else math.hypot(br.r_pu, br.x_pu)
                G.add_edge(br.start_node.uuid, br.end_node.uuid, weight=weight)

        # Slack bus detection
        slack_bus_name = "bus1"
        slack_node = next((n for n in system.nodes if n.name.lower() == slack_bus_name.lower()), None)
        if slack_node is None and system.nodes:
            slack_node = system.nodes[0]
            logging.warning(f"⚠️ Slack bus {slack_bus_name} not found; using {slack_node.name} as slack.")

        if not slack_node:
            logging.error("❌ No nodes available to compute distances.")
            return False

        slack_uuid = slack_node.uuid

        # Compute impedance distances from slack
        try:
            distances = nx.single_source_dijkstra_path_length(G, slack_uuid, weight="weight")
        except Exception as e:
            logging.warning(f"⚠️ Graph distance computation failed: {e}")
            distances = {n.uuid: float("inf") for n in system.nodes}

        # Record initial voltages
        initial_voltages = {}
        for bus in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus)
            volts = dss.Bus.Voltages()
            if not volts:
                continue
            v_real, v_imag = volts[0], volts[1]
            mag = math.hypot(v_real, v_imag)
            initial_voltages[bus.lower()] = mag

        # Sensitivity computation
        logging.info("🔁 Running sensitivity injections (using reusable generator)...")
        delta_v_acc = {n.uuid: [] for n in system.nodes}

        # Create a single reusable generator if not already present
        try:
            dss.Text.Command("Edit Generator.InjectQ kvar=0")
        except Exception:
            # Create fresh if doesn't exist
            dss.Text.Command("New Generator.InjectQ phases=3 bus1=bus1 kV=12.66 kW=0 kvar=0 model=3")

        for node in system.nodes:
            bus_name = node.name
            try:
                # Reset kvar to 0 before new injection
                dss.Text.Command("Edit Generator.InjectQ kvar=0")
                # Move generator to current bus and inject Q
                dss.Text.Command(f"Edit Generator.InjectQ bus1={bus_name} kvar={injected_kvar}")
                dss.Solution.Solve()

                # Record bus voltages after injection
                for bus in dss.Circuit.AllBusNames():
                    dss.Circuit.SetActiveBus(bus)
                    volts = dss.Bus.Voltages()
                    if not volts:
                        continue
                    v_real, v_imag = volts[0], volts[1]
                    mag = math.hypot(v_real, v_imag)
                    initial_mag = initial_voltages.get(bus.lower(), None)
                    if initial_mag is not None:
                        delta_v = abs(mag - initial_mag)
                        sys_node = next((n for n in system.nodes if n.name.lower() == bus.lower()), None)
                        if sys_node:
                            delta_v_acc[sys_node.uuid].append(delta_v)

            except Exception as e:
                logging.warning(f"⚠️ Injection at {bus_name} failed: {e}")

        # Reset generator to 0 kvar after sensitivity
        try:
            dss.Text.Command("Edit Generator.InjectQ kvar=0")
        except Exception:
            pass

        # Compute average ΔV per node
        avg_delta_v = {uuid: float(np.mean(deltas)) if deltas else 0.0
                       for uuid, deltas in delta_v_acc.items()}
        max_avg = max(avg_delta_v.values()) if avg_delta_v else 0.0

        sensitivity_scores = {uuid: 1.0 - (avgd / max_avg) if max_avg > 0 else 1.0
                              for uuid, avgd in avg_delta_v.items()}

        # Normalize impedance distances
        all_impedances = [distances.get(n.uuid, float("inf")) for n in system.nodes]
        finite_impedances = [v for v in all_impedances if math.isfinite(v)]
        z_min, z_max = (min(finite_impedances), max(finite_impedances)) if finite_impedances else (0.0, 1.0)

        impedance_scores = {}
        for n in system.nodes:
            val = distances.get(n.uuid, float("inf"))
            if not math.isfinite(val):
                impedance_scores[n.uuid] = 1.0
            elif z_max > z_min:
                impedance_scores[n.uuid] = (val - z_min) / (z_max - z_min)
            else:
                impedance_scores[n.uuid] = 0.0

        # Hop count normalization
        try:
            hop_counts = nx.single_source_shortest_path_length(G, slack_uuid)
        except Exception:
            hop_counts = {n.uuid: 999 for n in system.nodes}

        hop_values = [hop_counts.get(n.uuid, float("inf")) for n in system.nodes]
        finite_hops = [h for h in hop_values if math.isfinite(h)]
        min_hop, max_hop = (min(finite_hops), max(finite_hops)) if finite_hops else (0, 1)

        # Device config
        device_config = {}
        try:
            with open("/app/data/compensator_device.json", "r") as f:
                device_config = json.load(f)
                logging.info("📄 Loaded device config for ease-of-influence scoring.")
        except Exception:
            pass

        capacitor_reactive_power = device_config.get("capacitor_reactive_power", {})
        shunt_reactor_reactive_power = device_config.get("shunt_reactor_reactive_power", {})
        tie_switches = device_config.get("tie_switches", [])

        # Load scores
        load_mags = {n.uuid: math.hypot(n.power.real, n.power.imag) for n in system.nodes}
        max_load = max(load_mags.values()) if load_mags else 0.0
        min_load = min(load_mags.values()) if load_mags else 0.0

        load_scores = {uuid: ((mag - min_load) / (max_load - min_load)) if max_load > min_load else 0.0
                       for uuid, mag in load_mags.items()}

        # Combine scores
        for node in system.nodes:
            uuid, name = node.uuid, node.name
            r_imp = impedance_scores.get(uuid, 1.0)
            r_sens = sensitivity_scores.get(uuid, 1.0)
            r_elec = 0.6 * r_imp + 0.4 * r_sens

            load_score = load_scores.get(uuid, 0.0)
            hops = hop_counts.get(uuid, float("inf"))
            normalized_hops = (hops - min_hop) / (max_hop - min_hop) if math.isfinite(hops) and max_hop > min_hop else 1.0

            device_flag = 1.0 if (
                (isinstance(capacitor_reactive_power, dict) and name in capacitor_reactive_power) or
                (isinstance(shunt_reactor_reactive_power, dict) and name in shunt_reactor_reactive_power) or
                any(isinstance(pair, (list, tuple)) and name in pair for pair in tie_switches)
            ) else 0.0

            ease_score = 0.7 * (1 - normalized_hops) + 0.3 * device_flag

            node.r_imp = float(r_imp)
            node.r_sens = float(r_sens)
            node.load_score = float(load_score)
            node.ease_score = float(ease_score)

            priority_score = 0.4 * r_elec + 0.4 * load_score + 0.2 * ease_score
            logging.info(f"🏁 Bus {name} | R_elec={r_elec:.4f} | Load={load_score:.4f} | Ease={ease_score:.4f} | Priority={priority_score:.4f}")

        priority_score_exporter(system)
        return True

    except Exception as e:
        logging.error(f"❌ Priority computation failed: {e}")
        return False

# ================= Circuit Rebuild =================
MAX_RETRIES = 3
RETRY_DELAY_SEC = 2

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

def rebuild_circuit_from_excel():
    try:
        if not os.path.exists(EXCEL_PATH):
            logging.error(f"❌ Excel file not found: {EXCEL_PATH}")
            return

        xls = load_excel_with_retry(EXCEL_PATH)
        dss.Text.Command("Clear")
        dss.Basic.ClearAll()
        dss.Text.Command("New Circuit.RebuiltIEEE33 basekv=12.66 pu=1.0 phases=3 bus1=bus1")
        dss.Text.Command("Edit Vsource.Source bus1=bus1 phases=3 pu=1.0 basekv=12.66 angle=0")

        # Lines
        branch_df = pd.read_excel(xls, sheet_name="branches")
        for i, row in branch_df.iterrows():
            dss.Text.Command(
                f"New Line.L{i+1} Bus1={row['from']} Bus2={row['to']} Phases=3 R1={row['r']} X1={row['x']} C1={row['bch']}"
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

                # ---- Activate grid-tie switches ----
                for grid_name in activated_devices.get("grid_ties", []):
                    try:
                        if grid_name in comp_data.get("tie_impedance", {}):
                            r, x = comp_data["tie_impedance"][grid_name]
                            bus_from, bus_to = grid_name.split("-")
                            dss.Text.Command(
                                f"New Line.{grid_name} Bus1={bus_from} Bus2={bus_to} Phases=3 R1={r} X1={x} C1=0.0"
                            )
                            logging.info(f"✅ Grid-tie {grid_name} activated with R={r}, X={x}.")
                        else:
                            logging.warning(f"⚠️ No impedance data found for {grid_name}, skipping.")
                    except Exception as e:
                        logging.warning(f"⚠️ Could not activate grid-tie {grid_name}: {e}")

            except json.JSONDecodeError:
                logging.error(f"❌ Invalid JSON format in {ACTIVATED_DEVICES_PATH}. Skipping device activation.")
            except Exception as e:
                logging.error(f"❌ Failed to apply activated devices: {e}")
        else:
            logging.info(f"ℹ️ No activated_devices.json found, skipping activation.")

        # ================= Power Flow Solve =================
        dss.Text.Command("Set Voltagebases=[12.66]")
        dss.Text.Command("CalcVoltageBases")
        dss.Text.Command("Solve")
        if not dss.Solution.Converged():
            logging.warning("⚠️ Power flow did not converge, retrying...")
            dss.Text.Command("Solve Mode=Snap MaxControlIterations=20")

        logging.info(f"✅ Circuit rebuilt at {datetime.now().strftime('%H:%M:%S')}")
        for handler in logger.handlers:
            handler.flush()

    except Exception as e:
        logging.exception(f"❌ Error rebuilding circuit: {e}")
        for handler in logger.handlers:
            handler.flush()

def parse_and_export():
    try:
        # --- Clear and build circuit
        Sbase_MVA = 100.0
        line_length_km = 1.0  # assume each line is 1 km (or use real lengths if you have)

        # 🔧 Build base system from reusable module
        rebuild_circuit_from_excel()

        system = GridSystem()

        # --- Collect node data (same as before)
        for bus in dss.Circuit.AllBusNames():
            dss.Circuit.SetActiveBus(bus)
            volts = dss.Bus.Voltages()
            kv_base = dss.Bus.kVBase()
            if kv_base == 0 or not volts:
                continue

            v_real, v_imag = volts[0], volts[1]
            base_vll = kv_base * 1000
            pu_voltage = (v_real + 1j * v_imag) / base_vll

            # Sum load at this bus
            p_kw, q_kvar = 0, 0
            for load_name in dss.Loads.AllNames():
                dss.Loads.Name(load_name)
                try:
                    bus_connected = dss.CktElement.BusNames()[0].split('.')[0]
                except Exception:
                    bus_connected = ""
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
                voltage_imag=v_imag
            ))

        # --- Collect branch data (same as before)
        for line_name in dss.Lines.AllNames():
            dss.Lines.Name(line_name)
            buses = dss.CktElement.BusNames()
            if not buses or len(buses) < 2:
                continue
            from_bus, to_bus = buses[0].split('.')[0], buses[1].split('.')[0]
            r, x, bch, length = dss.Lines.R1(), dss.Lines.X1(), dss.Lines.C1(), dss.Lines.Length()
            z = complex(r, x)
            # z_base for per-unit
            z_base = (12.66 ** 2) / (Sbase_MVA * 1e6)
            # protect division by zero
            if z_base == 0:
                z_pu = complex(0.0, 0.0)
                r_pu = 0.0
                x_pu = 0.0
            else:
                z_pu = z / z_base
                r_pu = r / z_base
                x_pu = x / z_base

            system.branches.append(Branch(
                uuid=str(uuid.uuid4()),
                start_node=next((n for n in system.nodes if n.name.lower() == from_bus.lower()), None),
                end_node=next((n for n in system.nodes if n.name.lower() == to_bus.lower()), None),
                r=r, x=x, bch=bch, bch_pu=bch * length,
                length=length,
                base_voltage=12.66,
                base_apparent_power=Sbase_MVA * 1e6,
                r_pu=r_pu,
                x_pu=x_pu,
                z=z,
                z_pu=z_pu,
                type_="line"
            ))

        # Compute priority scores (and export priority sheet)
        compute_priority_scores(system, sbase_mva=Sbase_MVA, injected_kvar=500.0)

        return True

    except Exception as e:
        logging.error(f"Export failed: {e}")
        return False


@app.route('/health', methods=['GET'])
def health():
    return "🟢 main.py is live on port 4003", 200


if __name__ == "__main__":
    success = parse_and_export()
    if success:
        print("✅ Grid data and priority scores exported to Excel.")
    else:
        print("❌ Grid export or priority computation failed. See logs.")
    app.run(host="0.0.0.0", port=4003)
