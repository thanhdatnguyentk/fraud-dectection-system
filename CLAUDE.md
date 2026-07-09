# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

Local (single-host) Fraud Detection System (FDS) — a 5-phase implementation of the design in `plans/fraud-detection-system-design.md`. Target machine: 16 GB RAM, GTX 3050. Strict latency budget: scoring p99 < 50 ms.

Current state: Phase 1 (infra) ✅ + Phase 2 (streaming pipeline) ✅. Redpanda/Redis/Postgres stand up via `docker compose up -d`. The streaming pipeline (Producer → Redpanda → Stream Processor → Redis) is fully operational with proper sliding-window features. Model training and scoring API are not yet implemented (`scripts/train/` is empty).

## Common commands

```bash
# Activate venv (already present in repo)
source .venv/bin/activate

# Tests
pytest tests/ -v                              # full suite (33 tests, ~1s)
pytest tests/test_canonical_schema.py -v      # single file
pytest tests/test_e2e_pipeline.py::test_e2e_round_trip -v   # single test
pytest tests/ -k "fraud" -v                   # pattern match

# Data pipeline
python -m scripts.ingest.download_datasets --all              # 4 datasets (~2.4 GB raw)
python -m scripts.ingest.download_datasets --datasets sparkov # subset
python -m scripts.ingest.canonicalize_ieee_cis --raw data/raw/ieee_cis --out data/canonical/ieee_cis.parquet
python -m scripts.ingest.canonicalize_ulb --raw data/raw/ulb/creditcard.csv --out data/canonical/ulb.parquet
python -m scripts.ingest.canonicalize_sparkov --raw data/raw/sparkov/fraudTrain.csv --out data/canonical/sparkov.parquet
python -m scripts.ingest.canonicalize_paysim --raw data/raw/paysim --out data/canonical/paysim.parquet
python -m scripts.synth.generate_sample --rows 1000 --out data/canonical/sample.parquet

# Streaming pipeline (Phase 2)
python -m scripts.synth.kafka_producer --file data/canonical/sample.parquet --tps 200 --max 1000
python -m scripts.feature.stream_processor    # runs continuously, consumes from Redpanda, writes features to Redis
python -m scripts.synth.scenario_runner --scenario velocity_attack --tps 100
python -m scripts.synth.scenario_runner --scenario all --tps 200
python -m scripts.synth.verify_pipeline        # E2E smoke-test (requires stream_processor running)

# Infrastructure (from project root)
docker compose up -d       # Redpanda :19092, Redis :6379, Postgres :5432, Prometheus :9090, Grafana :3000
docker compose down -v     # nuke volumes
```

## Architecture (the big picture)

The system is a 7-stage pipeline laid out in `plans/data-synthesis-plan.md` §1.3. Three things are non-obvious and worth internalizing before touching code:

**1. Schema-first contract — `tx_canonical.v1`.** Every raw dataset (IEEE-CIS, ULB, Sparkov, PaySim, synthetic) must be normalized to this schema **before** touching any downstream component. Defined as a pandera schema in `scripts/canonical_schema.py` and enforced via `validate_canonical_df(df)` in every canonicalization script. If you add a new dataset, write a `scripts/ingest/canonicalize_<name>.py` that calls `validate_canonical_df` at the end — never skip it. Required columns: `tx_id` (ULID), `user_id` (64-hex), `dataset_source` (enum), `schema_version` (=1), `ts_ms` (int64), `amount_minor` (int64, **cents not dollars**), `currency`, `channel` (enum). Optional but typed: `device_fp`, `ip_hash`, `email_domain_hash`, `card_bin`, `merchant_id`, `mcc`, `country`, `lat`, `lon`, `label`, `attributes`. Schema version bumps are explicit — never silently widen.

**2. PII is hashed at the canonical boundary, never plaintext.** `scripts/common.py:hmac_hash(value, key)` produces 64-char HMAC-SHA256 hex. The key lives in `PII_HMAC_KEY` (env / `.env`, see `.env.example`). Use it for any field that needs joinability (`user_id`, `device_fp`, `email_domain_hash`, `ip_hash`). `user_id` for IEEE-CIS is `hmac_hash("card1=<card1>")`; for PaySim it's `hmac_hash("orig=<nameOrig>")` — the prefix prevents cross-dataset collisions when joined. `tx_id` is a ULID from `new_ulid()` (time-ordered, hand-rolled to avoid extra deps). Money is always `amount_minor` in **integer cents** — never floats, never strings.

**3. Nullable columns in pandas are a footgun.** When a canonical column is absent from the raw data (e.g. IEEE-CIS has no IP), do **not** assign Python `None` — pandas silently casts the whole column to `object` and pandera rejects the dtype. Use the explicit pattern: `pd.Series([float("nan")] * n, dtype="float64")` for floats, `pd.Series([None] * n, dtype="object")` for strings. The four canonicalize scripts all use this idiom. The `Int8` extension dtype (capital I) is required for `label` so missing values are `<NA>`, not `NaN`.

## Known gotchas

- **IEEE-CIS Kaggle slug**: the original `ieee-fraud-detection` returns 403 for most accounts. `scripts/ingest/download_datasets.py` uses the mirror `lixfemso/ieee-fraud-detection` (Apache-2.0, same files). Don't "fix" this back to the original slug.
- **IEEE-CIS has no ISO country**: `addr1` is a US ZIP prefix (3 digits), not an ISO-3166 code. It's kept in `attributes.addr1_zip_prefix`; `country` is NULL.
- **Sparkov timestamp**: pandas returns `datetime64[us, UTC]` (microsecond). Divide `// 1000` for ms, **not** `// 1_000_000`. The bug was fixed once; grep before re-editing.
- **PaySim filename varies**: Kaggle mirror ships as `PS_20174392719_<digits>_log.csv`. `canonicalize_paysim.py --raw data/raw/paysim` (a directory) auto-resolves the first `*.csv` inside. Pass a directory, not a hard-coded filename.
- **pandas FutureWarning**: `pandera.pandas` is the supported import path; importing from top-level `pandera` is deprecated. Already correct in `canonical_schema.py`.

## File map

- `scripts/canonical_schema.py` — single source of truth for `tx_canonical.v1` (pandera).
- `scripts/common.py` — `hmac_hash`, `new_ulid`, `Settings`, `load_settings`. Dependency-light (stdlib + pydantic).
- `scripts/ingest/download_datasets.py` — Kaggle CLI wrapper; skip-on-success via `_downloaded.marker`.
- `scripts/ingest/canonicalize_<dataset>.py` — one per dataset; output = single Parquet in `data/canonical/`.
- `scripts/synth/generate_sample.py` — deterministic synthetic generator (`--seed`), used for tests.
- `scripts/synth/kafka_producer.py` — high-throughput Parquet→Redpanda producer with batch rate-limiting (tested: 400+ TPS).
- `scripts/synth/scenario_runner.py` — fraud scenario generator (velocity_attack, impossible_travel, device_spray, fat_finger, burst_spike).
- `scripts/synth/verify_pipeline.py` — E2E pipeline verification (Producer→Redpanda→Processor→Redis).
- `scripts/feature/stream_processor.py` — Quix Streams consumer that computes 5 sliding-window features via Redis Sorted Sets and writes to `feat:user:{user_id}` hashes.
- `tests/test_canonical_schema.py` — 10 tests on schema rules (dtypes, regex, enums, ranges).
- `tests/test_common.py` — 8 tests on hashing/ULID determinism.
- `tests/test_stream_processor.py` — 7 tests on sliding-window feature logic (uses fakeredis, no Docker needed).
- `tests/test_data_quality.py`, `test_e2e_pipeline.py`, `test_scenarios.py` — pre-existing tests; do not delete.
- `docker-compose.yml` — Redpanda (Kafka API on 19092), Redis (6379), Postgres (5432, user=fds_admin/pw=fds_password/db=fds_db), Prometheus (9090), Grafana (3000).
- `.env.local` (gitignored) — local connection strings (KAFKA_BROKERS, REDIS_URL, POSTGRES_URL).
- `ops/prometheus.yml` — scrape config.
- `pipelines/` — placeholder for Airflow/Dagster DAGs (W12 of the 12-week data plan).
- `plans/data-synthesis-plan.md` — 12-week data roadmap (currently executing W1–W2).
- `plans/local-implementation-plan.md` — 5-phase local implementation roadmap.

## Conventions

- Python 3.12 (CI uses 3.10). Imports use `from __future__ import annotations`.
- All scripts are CLI tools invoked as `python -m scripts.<package>.<module>`. No `__main__` blocks fired on import.
- Tests live next to code they test, under `tests/`. Pytest fixtures in `conftest.py` if added.
- Raw data is immutable; canonical Parquet is regenerable. Both are gitignored.
- Kaggle credentials must live at `~/.kaggle/kaggle.json` (chmod 600) or as `KAGGLE_USERNAME`/`KAGGLE_KEY` env vars — never commit `kaggle.json` (already in `.gitignore`).
- Branching: per `.github/workflows/ci.yml`, **all PRs to `main` must originate from `staging`**. The CI job enforces this and will fail otherwise. Do not open PRs directly to `main`.

## Development workflow (MANDATORY)

1. **Test-First (TDD):** Luôn viết unit test TRƯỚC khi triển khai một tính năng. Test phải mô tả rõ hành vi mong đợi.
2. **Verify after implementation:** Sau khi triển khai xong, chạy `pytest tests/ -v` để đảm bảo tất cả tests (cũ + mới) đều PASSED. Không được bỏ qua bước này.
3. **Commit & push after each phase:** Khi hoàn thành xong một phase (hoặc một milestone quan trọng), phải commit với message rõ ràng và push lên remote repository. Format commit message: `feat(phase-N): <mô tả ngắn gọn>`.

## What is NOT yet built (do not assume it exists)

- No API server (FastAPI is planned in Phase 4).
- No offline feature engineering code (`scripts/feature/build_offline_features.py` does not exist).
- No model training code (`scripts/train/` is empty).
- No DAG definitions (`pipelines/` is empty).
- No rules engine — lands in Phase 4.
- No Redis backfill from offline features — lands in Phase 3.

If a task implies any of the above, implement the missing piece first; do not assume scaffolding.

## What IS built (Phase 1 + Phase 2)

- Docker Compose infra (Redpanda, Redis, Postgres, Prometheus, Grafana).
- 4 dataset canonicalization scripts + synthetic generator.
- Kafka producer with batch rate-limiting (400+ TPS tested).
- Stream processor with proper sliding-window features (Redis Sorted Sets).
- 5 real-time features: `tx_count_10m`, `amt_sum_1h`, `max_amt_1h`, `distinct_mcc_1h`, `seconds_since_last_tx`.
- Redis key convention: `feat:user:{user_id}` (Hash), `sw:tx:10m:{user_id}` / `sw:txdata:1h:{user_id}` (Sorted Sets).
- Scenario runner for fraud attack simulation.
- 40 unit/integration tests (all passing).