# ⚡ Integrated Active Distribution Management Framework  
### Volt–VAR Control | Network Reconfiguration | Auto-Reclosure | Service Restoration  

---

## 📌 System Overview

This repository presents a **hierarchical and event-driven Active Distribution Management Framework** developed on the IEEE-33 Bus Radial Distribution System.

The framework integrates:

- Volt–VAR Control and Network Reconfiguration (EnVVarco + NR)
- Trigger-Based Execution Module
- Auto-Reclosure (AR) Protection System
- Service Restoration (SR) Mechanism

Unlike conventional isolated implementations, this system coordinates **steady-state voltage regulation, protection logic, and post-fault restoration** within a unified Dockerized microservices architecture.

### Core Technologies

- OpenDSS for power flow simulation  
- Python (Flask-based services)  
- Docker containers for modular deployment  
- Shared Excel/JSON interfaces for inter-module communication  

---

# 🏗 System Architecture

The framework follows a **three-layer hierarchical control structure**:

### 1️⃣ Decision Layer
- Multi-objective optimization (Volt–VAR + reconfiguration)
- Voltage compliance monitoring (0.95–1.05 p.u.)
- Binary Fungal Growth Optimization (FGO)

### 2️⃣ Execution Layer
- Dedicated Trigger Module
- Sequential switching with stabilization delays
- Priority-based device activation

### 3️⃣ Protection & Restoration Layer
- Directional overcurrent relays
- Auto-reclosure logic (3 attempts)
- Topology-aware service restoration

---

# ⚙️ Core Modules

---

## 🔵 1. Volt–VAR Control & Network Reconfiguration (EnVVarco)

**Objective:** Maintain all bus voltages within 0.95–1.05 p.u. while minimizing switching operations.

### Key Features
- Real-time voltage monitoring
- Current threshold validation (< 250 A)
- Multi-objective optimization:
  - Maximize buses within voltage limits
  - Minimize activated devices
- OpenDSS validation
- Pareto-based solution selection
- Excel-based optimization summary export

> ⚠ EnVVarco does **not** execute switching directly.  
> It only produces validated control decisions.

---

## 🟠 2. Trigger Module

**Purpose:** Safe and time-coordinated execution of optimized control actions.

### Execution Strategy
- 45-second stabilization delay
- 20-second delay between switching actions
- Capacitors for undervoltage
- Reactors for overvoltage
- Tie-switch reconfiguration after compensation
- Priority-based device ranking using:
  - Voltage sensitivity
  - Electrical distance
  - Load impact

---

## 🔴 3. Auto-Reclosure (AR) Module

**Purpose:** Fast fault isolation and transient recovery.

### Protection Logic
- Directional overcurrent relays
- Pickup based on pre-fault current
- Distance-based time grading
- 50 ms evaluation cycle

### Reclosing Sequence
- 1st attempt → 1 sec  
- 2nd attempt → 5 sec  
- 3rd attempt → 15 sec  
- Lockout after 3 failed attempts  

Permanent faults trigger Service Restoration.

---

## 🟢 4. Service Restoration (SR) Module

**Purpose:** Restore supply to healthy sections after permanent fault isolation.

### Restoration Logic
- Real-time topology reconstruction
- Identify de-energized downstream region
- Select valid energized-to-deenergized tie
- Preserve radiality
- Validate with power flow after restoration

If restoration is infeasible, system exits safely without unsafe switching.

---

# 🐳 Dockerized Microservices Architecture

Each module runs as an independent Docker container:

| Module | Responsibility |
|--------|---------------|
| EnVVarco | Optimization decision-making |
| Trigger | Sequential switching execution |
| AR | Fault detection & reclosing |
| SR | Post-fault restoration |

### Advantages

- Fault containment  
- Modular scalability  
- Independent deployment  
- Reproducible simulations  
- Clear decision–execution separation  

Control transfer uses:
- Shared Excel/JSON state files  
- REST-based trigger signaling  

---

# 🔌 Test System: IEEE-33 Bus Network

- Nominal Voltage: 12.66 kV  
- 33 buses, 32 line segments  
- Balanced 3-phase loads  
- Fixed & controllable capacitor banks  
- Shunt reactors  
- Four normally-open tie-lines:
  - (25–29)
  - (12–22)
  - (18–33)
  - (8–21)

Simulation base:
- 12.66 kV  
- 100 MVA  

---

# 📊 Case Studies

### ✔ Case 1 – Undervoltage
- Lowest voltage: 0.9285 p.u.
- Capacitor + tie-switch activation
- Final minimum: 0.9656 p.u.

### ✔ Case 2 – Overvoltage
- Highest voltage: 1.0824 p.u.
- Reactor + tie-switch activation
- Final maximum: 1.0467 p.u.

### ✔ Case 3 – Fault in Ring Network
- Fault at Bus 15
- Multi-breaker isolation
- Loop-aware protection
- No additional restoration required

### ✔ Case 4 – Fault in Radial Network
- CB14 & CB15 isolation
- Tie (18–33) restoration
- Full service recovery
- No post-restoration violations

---

# 🔄 Control Flow

The framework operates in a hierarchical and event-driven manner.  
Control authority transitions automatically based on system conditions.

```
Normal Operation
↓
EnVVarco Optimization
↓
Trigger Module Execution
↓
If Abnormal Current or Fault Detected
↓
Auto-Reclosure (AR)
↓
If Fault is Transient → System Restored
If Fault is Permanent → Breaker Lockout
↓
Service Restoration (SR)
↓
Topology Reconfiguration (if feasible)
↓
Return to EnVVarco Monitoring
```

---

# 🧪 Technologies Used

- Python  
- Flask  
- OpenDSS  
- NumPy  
- Pandas  
- Docker  
- Grafana  
- Excel-based structured data interface  

---

# 🚀 How to Run

```bash
# Clone repository
git clone <repository-url>

# Navigate to project directory
cd <repository-folder>

# Build containers
docker-compose build

# Start services
docker-compose up
```

## Requirements

- Docker & Docker Compose installed

- OpenDSS dependencies included in container image

- Proper shared volume configuration

## 🎯 Key Contributions

- Hierarchical integration of voltage regulation, protection, and restoration

- Multi-objective Fungal Growth Optimization for Volt–VAR control

- Loop-aware fault isolation in reconfigured networks

- Trigger-based execution layer for operational realism

- Dockerized modular deployment for reproducibility

- Utility-aligned automation architecture

## 📌 Conclusion

This repository provides a modular, scalable, and utility-aligned Active Distribution Management framework integrating:

- Steady-state voltage control

- Protection coordination

- Fault isolation

- Automated service restoration

The architecture mirrors practical distribution automation systems and establishes a strong foundation for:

- Distributed Energy Resource (DER) coordination

- Adaptive protection settings

- Hardware-in-the-loop deployment

- Advanced grid automation research
