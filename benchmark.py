"""Compare FIFO and critical-path makespan on reproducible generated workloads."""
import argparse
import json
from pathlib import Path
import random
import statistics
import subprocess
import tempfile


def compare(binary: Path, path: Path, workers: int) -> dict:
    runs = {}
    for policy in ("fifo", "critical"):
        output = subprocess.check_output([str(binary), str(path), str(workers), policy], text=True)
        runs[policy] = json.loads(output)
    return {"fifo": runs["fifo"]["makespan"], "critical": runs["critical"]["makespan"],
            "lower_bound": runs["critical"]["fault_free_lower_bound"]}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("binary", type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve()
    results = []
    with tempfile.TemporaryDirectory(prefix="taskgraph-benchmark-") as directory:
        path = Path(directory) / "workload.tasks"
        for seed in range(40):
            rng = random.Random(seed)
            rows = []
            for i in range(40):
                deps = [f"job_{j:03}" for j in range(i) if rng.random() < 0.08]
                rows.append(f"job_{i:03} {rng.randint(1, 20)} 0 0 {','.join(deps) or '-'}")
            path.write_text("\n".join(rows) + "\n", encoding="utf-8")
            results.append({"seed": seed, **compare(binary, path, 4)})
    print(json.dumps({
        "description": "40 synthetic DAGs; 40 jobs; 4 workers; edges p=0.08; durations 1..20; no failures",
        "critical_wins": sum(r["critical"] < r["fifo"] for r in results),
        "ties": sum(r["critical"] == r["fifo"] for r in results),
        "critical_losses": sum(r["critical"] > r["fifo"] for r in results),
        "mean_fifo_makespan": statistics.mean(r["fifo"] for r in results),
        "mean_critical_makespan": statistics.mean(r["critical"] for r in results),
        "runs": results,
    }, indent=2))


if __name__ == "__main__":
    main()
