#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import os
import platform
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from eventflowsentry.baselines import run_baselines
from eventflowsentry.faults import apply_faults
from eventflowsentry.oracles import evaluate_oracles
from eventflowsentry.runner import scenario_grid
from eventflowsentry.serialization import event_to_json
from eventflowsentry.stats import mcnemar_exact, wilson_interval
from eventflowsentry.transport import kafka_round_trip
from eventflowsentry.workloads import generate_workload


def project_root() -> Path:
    here = Path(__file__).resolve()
    if (here.parent / "src" / "eventflowsentry").exists():
        return here.parent
    return here.parents[1]


ROOT = project_root()
RESULTS = ROOT / "results"
BASELINE_NAMES = (
    "window_aggregate_unit_check",
    "annotation_replay_check",
    "schema_contract_check",
)


def rate(values: list[bool]) -> float:
    return round(sum(values) / len(values), 6) if values else 0.0


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, round((pct / 100.0) * (len(ordered) - 1)))
    return round(ordered[idx], 6)


def scenario_digest(config) -> str:
    source = generate_workload(config.workload, config.n_events, config.seed)
    if config.fault is None:
        mutated, injection_log = list(source), []
    else:
        mutated, injection_log = apply_faults(source, config.fault)
    blob = b"\n".join(event_to_json(event) for event in mutated)
    blob += json.dumps(injection_log, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def reproducibility_summary(configs, trials: int = 5) -> dict:
    reference = {config.name: scenario_digest(config) for config in configs}
    comparisons = 0
    matches = 0
    for _ in range(trials - 1):
        for config in configs:
            comparisons += 1
            matches += int(scenario_digest(config) == reference[config.name])
    return {
        "scope": "deterministic_generation_and_mutation_digest_check",
        "trials": trials,
        "scenarios_checked": len(configs),
        "comparisons": comparisons,
        "matches": matches,
        "match_rate": rate([True] * matches + [False] * (comparisons - matches)) if comparisons else 0.0,
    }


def rate_intervals(values_by_method: dict[str, list[bool]]) -> dict[str, dict[str, float]]:
    return {
        name: wilson_interval(sum(values), len(values))
        for name, values in values_by_method.items()
    }


def wait_for_flink(rest_url: str, timeout_s: int = 90) -> dict:
    deadline = time.time() + timeout_s
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{rest_url.rstrip('/')}/overview", timeout=5) as response:
                data = json.loads(response.read().decode("utf-8"))
            if int(data.get("taskmanagers", 0)) >= 1 and int(data.get("slots-total", 0)) >= 1:
                return data
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_error = exc
            time.sleep(2)
    raise RuntimeError(f"Flink REST endpoint was not reachable at {rest_url}: {last_error}")


def main() -> int:
    RESULTS.mkdir(exist_ok=True)
    bootstrap = os.environ.get("EFS_KAFKA_BOOTSTRAP", "localhost:9092")
    flink_rest_url = os.environ.get("EFS_FLINK_REST_URL", "http://localhost:8081")
    run_id = os.environ.get("EFS_RUN_ID") or datetime.now(timezone.utc).strftime("docker-benchmark-%Y%m%dT%H%M%SZ")
    topic = f"efs-benchmark-{run_id.lower()}".replace("_", "-")

    flink_overview = wait_for_flink(flink_rest_url)
    configs = scenario_grid()
    source_by_name = {}
    fault_names_by_name = {}
    records = []
    started = time.perf_counter()
    for config in configs:
        source = generate_workload(config.workload, config.n_events, config.seed)
        if config.fault is None:
            mutated, injection_log = list(source), []
        else:
            mutated, injection_log = apply_faults(source, config.fault)
        source_by_name[config.name] = (config, source)
        fault_names_by_name[config.name] = {entry["fault"] for entry in injection_log}
        for event in mutated:
            records.append((config.name, event, {"workload": config.workload, "fault": config.fault.name if config.fault else "none"}))

    round_trip = kafka_round_trip(bootstrap_servers=bootstrap, topic=topic, records=records)
    scenario_rows = []
    method_hits: dict[str, list[bool]] = {"eventflowsentry": []}
    fault_method_hits: dict[str, list[bool]] = {"eventflowsentry": []}
    control_method_hits: dict[str, list[bool]] = {"eventflowsentry": []}
    fault_coverage: dict[str, dict[str, int]] = {}
    eval_durations_ms: list[float] = []
    for config in configs:
        cfg, source = source_by_name[config.name]
        consumed = sorted(round_trip.by_scenario.get(config.name, []), key=lambda event: (event.arrival_time_ms, event.event_id))
        fault_names = fault_names_by_name[config.name]
        eval_started = time.perf_counter()
        oracle_results = evaluate_oracles(
            source,
            consumed,
            fault_names,
            window_ms=cfg.window_ms,
            allowed_lateness_ms=cfg.allowed_lateness_ms,
        )
        baselines = run_baselines(source, consumed, window_ms=cfg.window_ms)
        eval_durations_ms.append((time.perf_counter() - eval_started) * 1000.0)
        efs_detected = any(result.detected_fault for result in oracle_results)
        is_control = cfg.fault is None
        method_hits["eventflowsentry"].append(efs_detected)
        (control_method_hits if is_control else fault_method_hits)["eventflowsentry"].append(efs_detected)
        fault_name = cfg.fault.name if cfg.fault else "none"
        if not is_control:
            coverage = fault_coverage.setdefault(fault_name, {"eventflowsentry": 0})
            if efs_detected:
                coverage["eventflowsentry"] += 1
        for name, detected in baselines.items():
            method_hits.setdefault(name, []).append(detected)
            (control_method_hits if is_control else fault_method_hits).setdefault(name, []).append(detected)
            if not is_control:
                coverage.setdefault(name, 0)
                if detected:
                    coverage[name] += 1
        scenario_rows.append(
            {
                "scenario": cfg.name,
                "workload": cfg.workload,
                "fault": fault_name,
                "is_control": is_control,
                "source_events": len(source),
                "events_consumed": len(consumed),
                "eval_duration_ms": round(eval_durations_ms[-1], 6),
                "eventflowsentry_detected": efs_detected,
                **baselines,
                "oracle_failures": ";".join(result.name for result in oracle_results if result.detected_fault),
            }
        )

    stats = {}
    efs = fault_method_hits["eventflowsentry"]
    for baseline in BASELINE_NAMES:
        stats[f"eventflowsentry_vs_{baseline}"] = mcnemar_exact(efs, fault_method_hits[baseline])

    elapsed = time.perf_counter() - started
    detection_rates = {name: rate(values) for name, values in fault_method_hits.items()}
    all_scenario_detection_rates = {name: rate(values) for name, values in method_hits.items()}
    control_false_positive_rates = {name: rate(values) for name, values in control_method_hits.items()}
    reproduction = reproducibility_summary(configs)
    summary = {
        "scope": "docker_kafka_flink_local_generated_workload_benchmark",
        "evidence_boundary": (
            "Generated workloads were transported through Kafka in Docker Compose while Flink cluster "
            "availability was verified through REST. Baseline checks are reproducible local controls "
            "over the same event streams, not vendor-certified Flink/Kafka Streams/Great Expectations suites."
        ),
        "run_id": run_id,
        "n_instances": len(configs),
        "n_fault_instances": sum(1 for cfg in configs if cfg.fault is not None),
        "n_control_instances": sum(1 for cfg in configs if cfg.fault is None),
        "n_workloads": len({cfg.workload for cfg in configs}),
        "workloads": sorted({cfg.workload for cfg in configs}),
        "event_sizes": sorted({cfg.n_events for cfg in configs}),
        "seed_start": min(cfg.seed for cfg in configs),
        "seed_end": max(cfg.seed for cfg in configs),
        "total_source_events": sum(cfg.n_events for cfg in configs),
        "total_consumed_events": round_trip.consumed,
        "n_baselines": 3,
        "baselines": list(BASELINE_NAMES),
        "methods": sorted(method_hits),
        "detection_rates": detection_rates,
        "detection_rate_wilson_ci": rate_intervals(fault_method_hits),
        "all_scenario_detection_rates": all_scenario_detection_rates,
        "control_false_positive_rates": control_false_positive_rates,
        "control_false_positive_wilson_ci": rate_intervals(control_method_hits),
        "statistical_tests": stats,
        "transport": {
            "kafka_bootstrap": bootstrap,
            "topic": topic,
            "produced_events": round_trip.produced,
            "consumed_events": round_trip.consumed,
            "round_trip_duration_s": round(round_trip.duration_s, 6),
            "end_to_end_duration_s": round(elapsed, 6),
            "events_per_second": round(round_trip.consumed / round_trip.duration_s, 3) if round_trip.duration_s else None,
        },
        "flink": {
            "rest_url": flink_rest_url,
            "overview": flink_overview,
            "validation_level": "cluster_rest_availability_only",
            "engine_job_executed": False,
        },
        "performance": {
            "mean_eval_duration_ms": round(sum(eval_durations_ms) / len(eval_durations_ms), 6),
            "p95_eval_duration_ms": percentile(eval_durations_ms, 95),
            "max_eval_duration_ms": round(max(eval_durations_ms), 6),
        },
        "reproducibility": reproduction,
        "fault_coverage_counts": fault_coverage,
    }

    summary_path = RESULTS / "docker_benchmark_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    rows_path = RESULTS / "docker_benchmark_instances.csv"
    with rows_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(scenario_rows[0]))
        writer.writeheader()
        writer.writerows(scenario_rows)

    manifest_path = RESULTS / "run_manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    else:
        manifest = {"runs": []}
    manifest.setdefault("runs", []).append(
        {
            "run_id": run_id,
            "scope": summary["scope"],
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "engine": "docker-compose",
            "model": None,
            "seed_start": min(cfg.seed for cfg in configs),
            "seed_end": max(cfg.seed for cfg in configs),
            "n_instances": len(configs),
            "outputs": [
                str(summary_path.relative_to(ROOT)),
                str(rows_path.relative_to(ROOT)),
            ],
            "python": platform.python_version(),
            "platform": platform.platform(),
            "cost_usd": 0.0,
        }
    )
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"summary": str(summary_path), "rows": str(rows_path), "n_instances": len(configs)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
