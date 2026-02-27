import opendssdirect as dss
import pandas as pd
import numpy as np
import time
import os
import logging
import sys
import json
from flask import Flask, jsonify, request
from datetime import datetime

# ---------------- Configuration ----------------
EXCEL_PATH = "/shared_volume/grid_data1.xlsx"
ACTIVATED_DEVICES_FILE = "/shared_volume/activated_devices.json"
VOLTAGE_TOL_PU = (0.95, 1.05)

logging.basicConfig(filename="trigger.log", level=logging.INFO)
app = Flask(__name__)

# ---------------- Helper to log voltages ----------------
def print_voltage_table(title):
    logging.info(f"\n=== {title.upper()} ===")
    logging.info(f"{'Bus':<8}{'Voltage (p.u.)':>15}")
    logging.info("-" * 25)
    for bus in dss.Circuit.AllBusNames():
        dss.Circuit.SetActiveBus(bus)
        vmag = dss.Bus.puVmagAngle()[0]
        logging.info(f"{bus:<8}{vmag:>15.4f}")

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

# ---------------- Excel-based System Builder ----------------
def build_circuit_from_excel():
    logging.info("🔄 Rebuilding IEEE-33 system from Excel...")

    try:
        if not os.path.exists(EXCEL_PATH):
            logging.error(f"❌ Excel file not found: {EXCEL_PATH}")
            return False

        xls = load_excel_with_retry(EXCEL_PATH)

        # Reset the DSS environment
        dss.Text.Command("Clear")
        dss.Basic.ClearAll()
        dss.Text.Command("New Circuit.IEEE33 basekv=12.66 pu=1.0 phases=3 bus1=bus1")
        dss.Text.Command("Edit Vsource.Source bus1=bus1 phases=3 pu=1.0 basekv=12.66 angle=0")

        # --- Add Lines ---
        if "branches" in xls.sheet_names:
            branch_df = pd.read_excel(xls, sheet_name="branches")
            for i, row in branch_df.iterrows():
                dss.Text.Command(
                    f"New Line.L{i+1} Bus1={row['from']} Bus2={row['to']} Phases=3 "
                    f"R1={row['r']} X1={row['x']} C1={row.get('bch', 0)}"
                )

        # --- Add Loads ---
        if "loads" in xls.sheet_names:
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

        # --- Read JSON data for compensators ---
        try:
            # If activated_devices.json doesn't exist yet, initialize empty dict
            if os.path.exists("/shared_volume/activated_devices.json"):
                with open("/shared_volume/activated_devices.json", "r") as f:
                    activated_devices = json.load(f)
            else:
                logging.warning("⚠️ Activated devices file not found. Starting with empty sets.")
                activated_devices = {"capacitors": [], "reactors": [], "grid_ties": []}

            # Compensator device data (must exist)
            with open("/app/data/compensator_device.json", "r") as f:
                comp_data = json.load(f)
            logging.info("✅ Loaded compensator data successfully.")

        except Exception as e:
            logging.error(f"❌ Failed to read compensator JSON files: {e}")
            return False


        # --- Add Activated Capacitors ---
        for bus in activated_devices.get("capacitors", []):
            kvar = comp_data["capacitor_reactive_power"].get(bus) or comp_data["fixed_capacitors"].get(bus)
            if kvar:
                logging.info(f"⚡ Adding Capacitor at {bus} ({kvar} kVAR)")
                dss.Text.Command(f"New Capacitor.Cap_{bus} Bus1={bus} Phases=3 kVAR={kvar} kV=12.66")
            else:
                logging.warning(f"⚠️ No capacitor rating found for {bus}")

        # --- Add Activated Reactors ---
        for bus in activated_devices.get("reactors", []):
            kvar = comp_data["shunt_reactor_reactive_power"].get(bus)
            if kvar:
                logging.info(f"⚡ Adding Reactor at {bus} ({kvar} kVAR)")
                dss.Text.Command(f"New Reactor.Reac_{bus} Bus1={bus} Phases=3 kVAR={kvar} kV=12.66")
            else:
                logging.warning(f"⚠️ No reactor rating found for {bus}")

        # --- Add Grid-Tie Lines ---
        for tie in activated_devices.get("grid_ties", []):
            if tie in comp_data["tie_impedance"]:
                r, x = comp_data["tie_impedance"][tie]
                bus1, bus2 = tie.split("-")
                logging.info(f"🔗 Adding Grid-Tie between {bus1} and {bus2} (R={r}, X={x})")
                dss.Text.Command(f"New Line.Tie_{bus1}_{bus2} Bus1={bus1} Bus2={bus2} Phases=3 R1={r} X1={x}")
            else:
                logging.warning(f"⚠️ Impedance not found for grid-tie {tie}")

        # --- Finalize and Solve ---
        dss.Text.Command("Set Voltagebases=[12.66]")
        dss.Text.Command("CalcVoltageBases")
        dss.Text.Command("Solve")

        if not dss.Solution.Converged():
            logging.warning("⚠️ Power flow did not converge. Retrying...")
            dss.Text.Command("Solve Mode=Snap MaxControlIterations=20")

        print_voltage_table("📊 Voltages AFTER system + device build")
        logging.info(f"✅ Circuit rebuilt successfully with compensators at {datetime.now().strftime('%H:%M:%S')}")
        return True

    except Exception as e:
        logging.exception(f"❌ Error building circuit: {e}")
        return False


# ---------------- Voltage Helper Functions ----------------
def get_voltages():
    data = {}
    for bus in dss.Circuit.AllBusNames():
        dss.Circuit.SetActiveBus(bus)
        mags = dss.Bus.puVmagAngle()[0:6:2]
        data[bus] = round(mags[0], 4)
    return data


def parse_impedance(imp_str):
    try:
        parts = imp_str.replace("R=", "").replace("X=", "").split(",")
        return float(parts[0]), float(parts[1])
    except:
        return 0.1, 0.3

# ---------------- Trigger Logic ----------------
def trigger_sequence():
    activated_devices = {"capacitors": [], "reactors": [], "grid_ties": []}

    priority_df = pd.read_excel("/shared_volume/priority_score.xlsx")
    opt_df = pd.read_excel("/shared_volume/voltage_optimization_summary.xlsx")

    devices_to_activate = opt_df[opt_df["Device Type"].isin(["Capacitor", "Reactor", "TapChanger"])]
    devices_sorted = devices_to_activate.merge(priority_df, left_on="Node", right_on="bus_name", how="left")
    devices_sorted = devices_sorted.sort_values(by="priority_score", ascending=False)

    gridtie_set = opt_df[opt_df["Device Type"] == "Grid-Tie"].copy()

    logging.info("Waiting 45 seconds before first compensation...")
    time.sleep(45)
    print_voltage_table("📊 Voltages AFTER first 45-sec wait")

    capacitor_set = devices_sorted[devices_sorted["Device Type"] == "Capacitor"].copy()
    reactor_set = devices_sorted[devices_sorted["Device Type"] == "Reactor"].copy()
    tap_set = devices_sorted[devices_sorted["Device Type"] == "TapChanger"].copy()

    for bus in tap_set["Node"]:
        dss.Text.Command(f"Edit Transformer.T_{bus} tap=1")

    while not capacitor_set.empty or not reactor_set.empty:
        # ✅ Read voltage data from the 'nodes' sheet in grid_data1.xlsx
        try:
            volt_df = pd.read_excel("/shared_volume/grid_data1.xlsx", sheet_name="nodes")

            # Map bus names to voltage_pu values
            voltages = dict(zip(volt_df["name"], volt_df["voltage_pu"]))

        except Exception as e:
            logging.error(f"❌ Failed to read voltages from Excel: {e}")
            voltages = {}


        undervolt_nodes = [bus for bus, v in voltages.items() if v < 0.95]
        overvolt_nodes = [bus for bus, v in voltages.items() if v > 1.05]

        logging.info(f"Voltage check: {len(undervolt_nodes)} undervolt, {len(overvolt_nodes)} overvolt")

        if len(undervolt_nodes) >= len(overvolt_nodes) and not capacitor_set.empty:
            row = capacitor_set.iloc[0]
            bus = row["Node"]
            kVAR = row["Reactive Power (MVAR)"]
            logging.info(f"⚡ Creating & activating Capacitor at {bus} ({kVAR} kVAR)")
            dss.Text.Command(f"New Capacitor.Cap_{bus} Bus1={bus} Phases=3 kVAR={kVAR} kV=12.66")
            activated_devices["capacitors"].append(bus)
            capacitor_set = capacitor_set[capacitor_set["Node"] != bus]

        elif len(overvolt_nodes) > len(undervolt_nodes) and not reactor_set.empty:
            row = reactor_set.iloc[0]
            bus = row["Node"]
            kVAR = row["Reactive Power (MVAR)"]
            logging.info(f"⚡ Creating & activating Reactor at {bus} ({kVAR} kVAR)")
            dss.Text.Command(f"New Reactor.Reac_{bus} Bus1={bus} Phases=3 kVAR={kVAR} kV=12.66")
            activated_devices["reactors"].append(bus)
            reactor_set = reactor_set[reactor_set["Node"] != bus]
        else:
            logging.info("✅ Voltages within limits or no remaining devices to apply.")
            break

        dss.Solution.Solve()
        print_voltage_table("📊 Voltages AFTER device activation")
        logging.info("Waiting 20 sec before next device...")
        time.sleep(20)

    # --- Apply Grid-Ties ---
    # --- Apply Grid-Ties Sequentially ---
    if not gridtie_set.empty:
        logging.info("🔁 Sequentially applying grid-tie switches (20 sec interval)...")
        for idx, row in gridtie_set.iterrows():
            tie_label = row["Node"]
            r_x = row["Reactive Power (MVAR)"]
            r, x = parse_impedance(r_x)
            bus1, bus2 = tie_label.split("-")
            line_name = f"Tie_{bus1}_{bus2}"

            logging.info(f"⚡ Activating Grid-Tie {line_name} between {bus1}-{bus2} (R={r}, X={x})")
            dss.Text.Command(f"New Line.{line_name} Bus1={bus1} Bus2={bus2} Phases=3 R1={r} X1={x} Enabled=yes")
            activated_devices["grid_ties"].append(tie_label)

            dss.Solution.Solve()
            print_voltage_table(f"📊 Voltages AFTER grid-tie {line_name} activation")

            if idx < len(gridtie_set) - 1:
                logging.info("⏳ Waiting 20 seconds before next grid-tie activation...")
                time.sleep(20)

    try:
        with open(ACTIVATED_DEVICES_FILE, "w") as f:
            json.dump(activated_devices, f, indent=2)
        logging.info(f"💾 Activated devices saved to {ACTIVATED_DEVICES_FILE}: {activated_devices}")
    except Exception as e:
        logging.error(f"❌ Failed to write activated devices file: {e}")

# ---------------- Flask Route ----------------
@app.route("/optimize", methods=["POST"])
def trigger():
    logging.info("📡 Trigger API called.")
    build_circuit_from_excel()
    trigger_sequence()
    return jsonify({
        "status": "Trigger sequence executed",
        "final_voltages": get_voltages()
    }), 200

if __name__ == "__main__":
    PORT = 4004
    logging.info(f"🚀 Starting Flask server on port {PORT}...")
    app.run(host="0.0.0.0", port=PORT)
