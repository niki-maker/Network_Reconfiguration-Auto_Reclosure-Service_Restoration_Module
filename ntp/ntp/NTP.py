import os
import time
import logging
import json
from pathlib import Path
import pandas as pd
from flask import Flask, jsonify, request
import threading
from influxdb_client import InfluxDBClient, Point, WriteOptions
from datetime import datetime, timezone
import re


# Flask app
app = Flask(__name__)

# Logging Setup
logging.basicConfig(filename="ntp.log", level=logging.INFO, filemode="w")

# InfluxDB Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "Hello")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "my-org")
TOKEN_FILE = "/token_storage/token.txt"
EXCEL_PATH = "/shared_volume/grid_data1.xlsx"

# Load token
if os.path.exists(TOKEN_FILE):
    with open(TOKEN_FILE, "r") as file:
        INFLUXDB_TOKEN = file.read().strip()
else:
    logging.error("Token file missing! Exiting.")
    exit(1)

# InfluxDB connection
def connect_to_influxdb():
    for _ in range(5):
        try:
            client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            logging.info("Connected to InfluxDB.")
            return client
        except Exception as err:
            logging.error(f"InfluxDB connection failed: {err}")
            time.sleep(5)
    logging.error("Unable to connect to InfluxDB after retries. Exiting.")
    exit(1)

client = connect_to_influxdb()
write_api = client.write_api(write_options=WriteOptions(batch_size=1))

# In-memory data cache
node_data = []
branch_data = []

def normalize_bus(bus_name: str) -> str:
    """
    Collapses OpenDSS internal nodes like bus1_cb, bus1_sw, bus1_br
    into the parent electrical bus name (bus1).
    """
    return re.sub(r'_(cb|sw|br|breaker|switch).*$', '', bus_name, flags=re.IGNORECASE)

def is_cb_bus(bus_name: str) -> bool:
    return bool(re.search(r'_(cb|sw|br|breaker|switch)', bus_name, re.IGNORECASE))

def load_excel_data():
    global node_data, branch_data
    if not os.path.exists(EXCEL_PATH):
        logging.error("Excel file not found.")
        node_data = []
        branch_data = []
        return False
    try:
        df_nodes = pd.read_excel(EXCEL_PATH, sheet_name="nodes")
        df_branches = pd.read_excel(EXCEL_PATH, sheet_name="branches")
        node_data = df_nodes.to_dict(orient="records")
        branch_data = df_branches.to_dict(orient="records")
        logging.info("✅ Excel data reloaded into memory.")
        return True
    except Exception as e:
        logging.error(f"❌ Failed to reload Excel: {e}")
        node_data = []
        branch_data = []
        return False

def ntp_powerflow():
    logging.info("🔁 Starting NTP telemetry push to InfluxDB...")
    if not load_excel_data():
        logging.error("Excel load failed, skipping InfluxDB push.")
        return

    for node in node_data:
        point = (
            Point("node")
            .tag("name", node["name"])
            .tag("site", "MainGrid")                 # fixed tag to group all nodes
            .field("voltage_pu_mag", node["voltage_pu"])
            .field("voltage_pu_phase_deg", node["voltage_angle_deg"])
            .field("load_pu_mag", node["power_pu"])
            .field("load_pu_angle_deg", node["power_angle_deg"])
            .field("base_voltage", node["base_voltage"])
            .field("base_apparent_power", node["base_apparent_power"])
            .field("load_mag", node["real_power"])
            .field("load_angle_deg", node["imag_power"])
            .field("voltage_real", node["voltage_real"])
            .field("voltage_imag", node["voltage_imag"])
        )
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

    for branch in branch_data:
        measurement = "transformer" if branch["type"] == "transformer" else "branch"
        point = (
            Point(measurement)
            .tag("uuid", branch["uuid"])
            .tag("from", branch["from"])
            .tag("to", branch["to"])
            .field("r", branch["r"])
            .field("x", branch["x"])
            .field("base_voltage", branch["base_voltage"])
            .field("base_apparent_power", branch["base_apparent_power"])
            .field("r_pu", branch["r_pu"])
            .field("x_pu", branch["x_pu"])
            .field("z_real", branch["z_real"])
            .field("z_imag", branch["z_imag"])
            .field("z_pu_real", branch["z_pu_real"])
            .field("z_pu_imag", branch["z_pu_imag"])
            .field("bch", branch["bch"])
            .field("bch_pu", branch["bch_pu"])
            .field("length", branch["length"])
            .field("short_circuit_temp", branch.get("short_circuit_temp", 0.0))
        )
        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=point)

    logging.info("✅ Grid telemetry posted to InfluxDB.")

def continuous_telemetry_loop():
    while True:
        try:
            ntp_powerflow()
        except Exception as e:
            logging.error(f"⚠️ Exception in continuous loop: {e}")
        time.sleep(0.5)

@app.route("/run", methods=["POST"])
def trigger():
    threading.Thread(target=ntp_powerflow).start()
    return jsonify({"status": "NTP module started"}), 200

@app.route("/reload", methods=["POST"])
def reload_data():
    success = load_excel_data()
    if success:
        return jsonify({"status": "Reload successful"}), 200
    else:
        return jsonify({"error": "Reload failed"}), 500

@app.route("/grafana_data", methods=["GET"])

@app.route("/grafana_data", methods=["GET"])
def grafana_data():
    if not node_data or not branch_data:
        return jsonify({"error": "Data not loaded"}), 500

    connections = []
    nodes = []

    # ---------------- Load capacitor/reactor/transformer config ----------------
    capacitor_reactive_power = {}
    shunt_reactor_reactive_power = {}
    transformers = []
    fixed_capacitors = {}
    try:
        json_path = Path("/app/data/compensator_device.json")  # volume mount path
        with open(json_path, "r") as f:
            device_config = json.load(f)
        capacitor_reactive_power = device_config.get("capacitor_reactive_power", {})
        shunt_reactor_reactive_power = device_config.get("shunt_reactor_reactive_power", {})
        transformers = device_config.get("transformers", [])
        fixed_capacitors = device_config.get("fixed_capacitors", {})
        logging.info(f"📄 Loaded device config from {json_path}")
    except Exception as e:
        logging.error(f"❌ Failed to load device config: {e}")

    # ---------------- Load activated devices list ----------------
    activated_devices = {"capacitors": [], "reactors": [], "transformers": []}
    try:
        activated_file = Path("/shared_volume/activated_devices.json")
        if activated_file.exists():
            with open(activated_file, "r") as f:
                activated_devices = json.load(f)
            logging.info(f"✅ Loaded activated devices: {activated_devices}")
    except Exception as e:
        logging.error(f"❌ Failed to load activated devices file: {e}")

    # ---------------- Branch + Circuit Breaker handling ----------------
    transformer_pairs = {
        tuple(sorted((normalize_bus(tx['from']), normalize_bus(tx['to']))))
        for tx in transformers
    }


    breaker_map = {}     # (busA, busB) → CB_id
    normal_lines = []    # lines without breakers

    for branch in branch_data:
        frm_raw = branch["from"]
        to_raw = branch["to"]

        # Skip transformer electrical branches
        if (frm_raw, to_raw) in transformer_pairs or (to_raw, frm_raw) in transformer_pairs:
            continue

        frm = normalize_bus(frm_raw)
        to = normalize_bus(to_raw)

        if frm == to:
            continue

        # Detect breaker existence
        if is_cb_bus(frm_raw) or is_cb_bus(to_raw):
            key = tuple(sorted((frm, to)))
            breaker_map[key] = {
                "cb_id": f"CB_{frm}_{to}",
                "from": frm,
                "to": to,
                "branch": branch
            }
        else:
            normal_lines.append((frm, to, branch))


    used_pairs = set()

    cb_counter = 1

    for (busA, busB), data in breaker_map.items():
        cb_id = data["cb_id"]

        key = tuple(sorted((busA, busB)))
        has_tx = key in transformer_pairs

        # ---- CB node ----
        nodes.append({
            "id": cb_id,
            "label": f"CB{cb_counter}",
            "icon": "square",
            "size": 22,
            "color": "orange",
            "Message": f"Circuit Breaker between {busA} and {busB}"
        })

        cb_counter += 1


        # ---- busA → CB ----
        connections.append({
            "id": f"{cb_id}_in",
            "source": busA,
            "target": cb_id,
            "thickness": 7,
            "color": "orange"
        })

        if has_tx:
            # ===== CB → TX → busB =====
            tx = next(
                tx for tx in transformers
                if tuple(sorted((normalize_bus(tx["from"]), normalize_bus(tx["to"])))) == key
            )

            tx_id = f"TX_{tx['from']}_{tx['to']}"

            # ---- Transformer node ----
            nodes.append({
                "id": tx_id,
                "label": f"Transformer {tx['from']}→{tx['to']}",
                "color": "white",
                "size": 20,
                "icon": "adjust-circle",
                "Message": f"Transformer {tx['name']}"
            })

            connections.append({
                "id": f"{cb_id}_to_tx",
                "source": cb_id,
                "target": tx_id,
                "thickness": 7,
                "color": "orange"
            })

            connections.append({
                "id": f"{tx_id}_out",
                "source": tx_id,
                "target": busB,
                "thickness": 7,
                "color": "orange"
            })

        else:
            # ---- CB → busB (no transformer) ----
            connections.append({
                "id": f"{cb_id}_out",
                "source": cb_id,
                "target": busB,
                "thickness": 7,
                "color": "orange"
            })

        used_pairs.add(key)


    for frm, to, branch in normal_lines:
        key = tuple(sorted((frm, to)))
        if key in used_pairs:
            continue

        connections.append({
            "id": branch["uuid"],
            "source": frm,
            "target": to,
            "base_voltage": f"{branch['base_voltage']} kV",
            "reactance_pu": round(branch["x"], 5),
            "resistance_pu": round(branch["r"], 5),
            "shunt_susceptance_pu": round(branch["bch_pu"], 5),
            "thickness": 7,
            "Message": (
                f"Base voltage: {branch['base_voltage']} kV; "
                f"X: {round(branch['x'], 5)}; "
                f"R: {round(branch['r'], 5)}"
            )
        })



    # ---------------- Main grid nodes ----------------
    color_palette = [
        "red", "orange", "yellow", "green", "blue", "indigo", "violet", "pink",
        "brown", "cyan", "lime", "magenta", "gold", "silver", "teal"
    ]

    for i, node in enumerate(node_data):

        # 🚫 Skip OpenDSS internal CB / switch buses
        if is_cb_bus(node["name"]):
            continue

        present_voltage = f"{node['voltage_pu']:.4f}∠{node['voltage_angle_deg']:.2f}°"
        timestamp = datetime.now(timezone.utc).isoformat()

        node_obj = {
            "base_voltage": f"{node['base_voltage']} kV",
            "id": node["name"],
            "label": f"Node {node['name']}",
            "present_voltage": present_voltage,
            "timestamp": timestamp,
            "color": color_palette[i % len(color_palette)],
            "size": 40
        }
        nodes.append(node_obj)


    # ---------------- Variable Capacitors ----------------
    for cap_node, q_mvar in capacitor_reactive_power.items():
        cap_id = f"VarCap_{cap_node}"
        is_active = cap_node in activated_devices.get("capacitors", [])
        nodes.append({
            "id": cap_id,
            "label": f"Variable Capacitor {cap_node}",
            "color": "green" if is_active else "white",
            "size": 20,
            "icon": "table-expand-all",
            "Message": f"Variable Capacitor-{cap_node}-{q_mvar} MVAR"
        })
        connections.append({
            "id": f"conn_{cap_id}",
            "source": cap_id,
            "target": cap_node,  # <- must point to real bus node
            "thickness": 7 if is_active else 1,
            "color": "green" if is_active else "grey",
            "Message": f"Variable Capacitor-{cap_node}-{q_mvar} MVAR"
        })

    # ---------------- Fixed Capacitors ----------------
    for fixed_node, q_mvar in fixed_capacitors.items():
        fixed_id = f"FixedCap_{fixed_node}"
        nodes.append({
            "id": fixed_id,
            "label": f"Fixed Capacitor {fixed_node}",
            "color": "white",
            "size": 20,
            "icon": "pause",
            "Message": f"Fixed Capacitor-{fixed_node}-{q_mvar} MVAR"
        })
        connections.append({
            "id": f"conn_{fixed_id}",
            "source": fixed_id,
            "target": fixed_node,  # <- must point to real bus node
            "thickness": 7,
            "color": "green",
            "Message": f"Fixed Capacitor-{fixed_node}-{q_mvar} MVAR"
        })



    # ---------------- Reactors as separate nodes ----------------
    for reac_node, q_mvar in shunt_reactor_reactive_power.items():
        reac_id = f"Reac_{reac_node}"
        is_active = reac_node in activated_devices.get("reactors", [])
        nodes.append({
            "id": reac_id,
            "label": f"Reactor {reac_node}",
            "color": "green" if is_active else "white",
            "size": 20,
            "icon": "link",
            "Message": f"Reactor-{reac_node}-{q_mvar} MVAR"
        })
        connections.append({
            "id": f"conn_{reac_id}",
            "source": reac_id,
            "target": reac_node,
            "thickness": 7 if is_active else 1,
            "color": "green" if is_active else "grey",
            "Message": f"Reactor-{reac_node}-{q_mvar} MVAR"
        })



    # ---------------- Insert Transformer Nodes ----------------
    for tx in transformers:
        key = tuple(sorted((normalize_bus(tx['from']), normalize_bus(tx['to']))))
        if key in used_pairs:
            continue
        tx_node_id = f"TX_{tx['from']}_{tx['to']}"
        is_active = tx.get("name") in activated_devices.get("transformers", [])
        nodes.append({
            "id": tx_node_id,
            "label": f"Transformer {tx['from']}→{tx['to']}",
            "color": "green" if is_active else "white",
            "size": 20,
            "icon": "adjust-circle",
            "Message": (
                f"Transformer {tx['name']} | {tx['rating_kVA']} kVA | "
                f"{tx['kv_primary']}kV/{tx['kv_secondary']}kV | "
                f"R%: {tx['r_percent']}"
            )
        })
        connections.append({
            "id": f"{tx_node_id}_1",
            "source": tx['from'],
            "target": tx_node_id,
            "thickness": 7,
            "color": "orange" if not is_active else "green",
            "Message": f"{tx['name']} Primary Side"
        })
        connections.append({
            "id": f"{tx_node_id}_2",
            "source": tx_node_id,
            "target": tx['to'],
            "thickness": 7,
            "color": "orange" if not is_active else "green",
            "Message": f"{tx['name']} Secondary Side"
        })

    return jsonify({
        "connections": connections,
        "nodes": nodes
    })



@app.route("/health", methods=["GET"])
def health():
    return "🟢 NTP module is live", 200

# Start loop at startup
threading.Thread(target=continuous_telemetry_loop, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=4000)