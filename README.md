# EventFlowSentry

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

EventFlowSentry provides a reproducible local-first workflow for generating event-time stream faults, transporting mutated events through Kafka-compatible infrastructure, and evaluating metamorphic correctness oracles.

Designed for researchers and engineers studying streaming-pipeline correctness, CI regression testing, and reproducible fault-injection workflows (e.g., lateness, loss, replay, duplicate delivery, clock skew, schema rollback).

## Installation

From a source checkout:

```bash
python -m pip install -e .
```

PyPI packaging is planned for the tagged public release. Until that release exists, use an editable source install.

## Quickstart

Run a local Docker/Kafka fault-injection benchmark:

```bash
cd eventflowsentry

python -m pip install -e ".[dev]"
pytest tests/ -q

# Run a deterministic local scenario preview
eventflowsentry scenario --workload payments --fault late --events 100 --seed 1

# Run the Docker/Kafka/Flink-availability benchmark
make docker-benchmark
```

## Fault Coverage

EventFlowSentry validates streaming applications across several fault families:
- **Lateness:** Late-arriving events across watermark boundaries.
- **Loss:** Intermittent drops of essential events.
- **Replay:** Crash-recovery simulation resulting in replayed batches.
- **Duplicate:** Network retries resulting in exact duplicates.
- **Clock Skew:** Misaligned event timestamps vs processing time.
- **Schema Drift:** Sudden backward/forward schema changes.

## Citation

If you use EventFlowSentry in your research, cite the archived release DOI once it is available. A draft citation record is included in `CITATION.cff`.

Current manuscript draft:

```bibtex
@article{adwant2026eventflowsentry,
  title={EventFlowSentry: Reproducible Fault Injection and Metamorphic Testing for Event-Time Streaming Pipelines},
  author={Adwant, Goutam},
  journal={SoftwareX},
  year={2026},
  publisher={Elsevier}
}
```

## Documentation

- `docs/BENCHMARK.md` describes Docker prerequisites, expected runtime, outputs, and known evidence boundaries.
- `docs/API.md` summarizes the Python API and command-line entry point.
