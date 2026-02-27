import pandas as pd
import numpy as np
import logging

def export_grid_to_excel(system, path="/shared_volume/grid_data.xlsx"):
    try:
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
                "reactive_power": node.reactive_power  # ✅ Added field
            })
        node_df = pd.DataFrame(node_records)

        branch_records = []
        for branch in system.branches:
            branch_records.append({
                "uuid": branch.uuid,
                "from": branch.start_node.name if branch.start_node else "Unknown",
                "to": branch.end_node.name if branch.end_node else "Unknown",
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
                "type": "transformer" if "TR" in branch.uuid else "line"
            })
        branch_df = pd.DataFrame(branch_records)

        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            node_df.to_excel(writer, sheet_name="nodes", index=False)
            branch_df.to_excel(writer, sheet_name="branches", index=False)

        logging.info(f"📁 Excel exported to {path}")
        return True
    except Exception as e:
        logging.error(f"❌ Excel export failed: {e}")
        return False