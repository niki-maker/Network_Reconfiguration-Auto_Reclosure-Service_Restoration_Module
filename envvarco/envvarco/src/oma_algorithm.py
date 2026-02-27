import numpy as np
import logging
import opendssdirect as dss
import multiprocessing

logging.basicConfig(level=logging.INFO)

# ---------------- Circuit Creation ---------------- #
def create_circuit(name="IEEE33"):
    logging.info(f"🔧 Creating new circuit: {name}")
    dss.Basic.ClearAll()
    dss.Text.Command(f"New Circuit.{name} basekv=12.66 pu=1.0")

# ---------------- Device Initialization ---------------- #
def initialize_devices(capacitor_buses, reactor_buses, tie_switches, tie_impedance):
    logging.info("⚙️ Initializing capacitors, reactors, and tie switches...")

    for bus, kvar in capacitor_buses.items():
        cap_name = f"Cap_{bus}"
        dss.Text.Command(f"New Capacitor.{cap_name} bus1={bus} phases=3 kvar={kvar} kv=12.66 enabled=no")

    for bus, kvar in reactor_buses.items():
        reac_name = f"Reac_{bus}"
        dss.Text.Command(f"New Reactor.{reac_name} Bus1={bus} Phases=3 kvar={kvar} kv=12.66 enabled=no")

    for tie in tie_switches:
        bus1, bus2 = tie
        name = f"Tie_{bus1}_{bus2}"
        r, x = tie_impedance.get(f"{bus1}-{bus2}", [0.1, 0.3])  # Optional fallback
        dss.Text.Command(f"New Line.{name} Bus1={bus1} Bus2={bus2} Phases=3 R1={r} X1={x} Enabled=no")

# ---------------- Objective Function ---------------- #
def combined_cap_reac_objective_opendss(solution, capacitor_buses, reactor_buses, tie_switches, tie_impedance):
    try:
        # --- Reset to base case first ---
        for bus in capacitor_buses:
            dss.Text.Command(f"Edit Capacitor.Cap_{bus} enabled=no")
        for bus in reactor_buses:
            dss.Text.Command(f"Edit Reactor.Reac_{bus} enabled=no")
        for tie in tie_switches:
            bus1, bus2 = tie
            name = f"Tie_{bus1}_{bus2}"
            dss.Text.Command(f"Edit Line.{name} enabled=no")

        solution_bin = np.where(solution >= 0.5, 1, 0)
        cap_keys = list(capacitor_buses.keys())
        reac_keys = list(reactor_buses.keys())
        tie_keys = [f"{bus1}-{bus2}" for bus1, bus2 in tie_switches]

        # --- Apply candidate temporarily ---
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

        if not dss.Solution.Converged():
            return [0, 1e6]  # Penalize infeasible solution

        voltages = np.array(dss.Circuit.AllBusMagPu())
        buses_in_limit = np.sum((voltages >= 0.95) & (voltages <= 1.05))
        wear = np.sum(solution_bin)

        result = [buses_in_limit, wear]

        # --- Restore base case after evaluation ---
        for bus in capacitor_buses:
            dss.Text.Command(f"Edit Capacitor.Cap_{bus} enabled=no")
        for bus in reactor_buses:
            dss.Text.Command(f"Edit Reactor.Reac_{bus} enabled=no")
        for tie in tie_switches:
            bus1, bus2 = tie
            name = f"Tie_{bus1}_{bus2}"
            dss.Text.Command(f"Edit Line.{name} enabled=no")

        dss.Text.Command("Solve Mode=Snap")

        return result

    except Exception as e:
        logging.error(f"❌ Objective function error: {e}")
        return [0, 1e6]

# ---------------- Pareto Utilities ---------------- #
def update_pareto_archive(new_solution, archive):
    to_remove = []
    for idx, archived in enumerate(archive):
        if dominates(archived[-2:], new_solution[-2:]):
            return archive
        elif dominates(new_solution[-2:], archived[-2:]):
            to_remove.append(idx)
    archive = [archived for i, archived in enumerate(archive) if i not in to_remove]
    archive.append(new_solution)
    return archive

def dominates(obj1, obj2):
    # Maximize buses in limit, minimize wear
    return (obj1[0] >= obj2[0] and obj1[1] <= obj2[1]) and (obj1[0] > obj2[0] or obj1[1] < obj2[1])

def extract_pareto_front(archive):
    non_dominated = []
    for sol in archive:
        if not any(dominates(other[-2:], sol[-2:]) for other in archive if other is not sol):
            non_dominated.append(sol)
    return non_dominated

# ---------------- Binary Fungal Growth Optimizer ---------------- #
def fungal_growth_optimizer(N, Tmax, dim, fobj):
    M, Ep, R = 0.6, 0.7, 0.9
    # Binary population: 0 or 1
    S = np.random.randint(0, 2, (N, dim))
    pareto_archive = []
    obj_cache = {}

    def eval_with_cache(sol):
        # Convert to binary key
        key = tuple(sol.astype(int))
        if key in obj_cache:
            return obj_cache[key]
        val = fobj(sol)
        if val is None:
            val = [float("inf"), float("inf")]
        obj_cache[key] = val
        return val

    # Evaluate initial population
    for sol in S:
        objs = eval_with_cache(sol)
        pareto_archive = update_pareto_archive(np.hstack((sol, objs)), pareto_archive)

    for t in range(Tmax):
        nutrients = np.random.rand(N) if t <= Tmax / 2 else np.array([sol[-2] for sol in pareto_archive])
        nutrients = nutrients.astype(float)
        nutrients /= (np.sum(nutrients) + 2 * np.random.rand())

        for i in range(N):
            a, b, c = np.random.choice([x for x in range(N) if x != i], 3, replace=False)
            p = np.random.rand()
            Er = M + (1 - t / Tmax) * (1 - M)

            new_sol = S[i].copy()

            if p < Er:
                # Exploration: flip bits probabilistically
                F = np.random.rand() * (1 - t / Tmax) ** (1 - t / Tmax)
                r1, r2 = np.random.rand(dim), np.random.rand()
                U1 = r1 < r2
                new_sol = U1.astype(int) * new_sol + (1 - U1.astype(int)) * (new_sol ^ (S[a] ^ S[b]))
            else:
                # Exploitation: bit-wise adjustment using neighbors
                De = ((np.random.rand(dim) - 0.5) * (S[a] ^ S[b])).astype(int)
                if np.random.rand() < np.random.rand():
                    De2 = (np.random.rand(dim) * (new_sol ^ S[c]) * (np.random.rand(dim) > np.random.rand())).astype(int)
                    new_sol = ((new_sol ^ De2) & 1) ^ De
                else:
                    De3 = ((np.random.rand(dim) * (S[a] ^ new_sol) + np.random.rand(dim) * ((np.random.rand(dim) > 0.5) * S[c] ^ new_sol))).astype(int)
                    new_sol = (new_sol ^ De3) & 1

            # Ensure binary
            new_sol = np.clip(new_sol, 0, 1)
            S[i] = new_sol

            objs = eval_with_cache(S[i])
            pareto_archive = update_pareto_archive(np.hstack((S[i], objs)), pareto_archive)

        # Stop if all possible solutions evaluated
        if len(obj_cache) >= 2 ** dim:
            break

        logging.info(f"📈 Iteration {t+1}/{Tmax} | Pareto solutions: {len(pareto_archive)}")

    # Extract Pareto front
    pareto_front = extract_pareto_front(pareto_archive)
    if not pareto_front:
        dummy_sol = np.zeros(dim)
        pareto_front = [np.hstack((dummy_sol, [float("inf"), float("inf")]))]

    # Select best solution: maximize buses in limit, then minimize wear
    buses_in_limit = np.array([sol[-2] for sol in pareto_front])
    wear = np.array([sol[-1] for sol in pareto_front])
    max_buses = np.max(buses_in_limit)
    candidates = [sol for sol in pareto_front if sol[-2] == max_buses]
    min_wear_idx = np.argmin([sol[-1] for sol in candidates])
    best_solution = candidates[min_wear_idx]

    return pareto_front, best_solution
