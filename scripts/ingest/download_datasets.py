"""Download the four datasets referenced in ``plans/data-synthesis-plan.md``.

Datasets
--------
1. IEEE-CIS Fraud Detection   — Kaggle : `ieee-fraud-detection`
2. ULB Credit Card Fraud      — Kaggle : `creditcardfraud` (alias `mlg-ulb/creditcardfraud`)
3. Sparkov Synthetic          — GitHub : `NameerAhmad/SparkovSyntheticDataset`
4. PaySim                     — Kaggle : `paysim1`

Behavior
--------
* Each dataset is downloaded into ``<raw>/<name>/`` only if not already
  present (skip-on-success — ``_downloaded.marker`` file).
* Kaggle requires either ``~/.kaggle/kaggle.json`` *or* ``KAGGLE_USERNAME`` +
  ``KAGGLE_KEY`` environment variables.
* Non-Kaggle downloads use plain ``requests`` with a visible progress bar
  (``tqdm``).
* The script is safe to re-run: it never re-downloads a complete file.

Run::

    python -m scripts.ingest.download_datasets --datasets ieee_cis,ulb
    python -m scripts.ingest.download_datasets --all
"""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

import requests
from tqdm import tqdm

from scripts.common import load_settings


# =============================================================================
# Dataset descriptor
# =============================================================================

@dataclass
class DatasetSpec:
    key: str                                       # logical key
    display: str                                    # human-readable name
    source: str                                     # "kaggle" | "url" | "github"
    target_dir: Path                                # resolved at runtime
    kaggle_slug: str | None = None                  # "owner/dataset" for kaggle
    files: list[tuple[str, str]] = field(default_factory=list)
    # files: list of (url-or-kaggle-relative-path, dest-name)
    github_repo: tuple[str, str] | None = None      # (owner, repo) for git clone
    github_subpath: str | None = None
    size_hint_mb: int = 0
    description: str = ""


def _specs(target_root: Path) -> list[DatasetSpec]:
    return [
        DatasetSpec(
            key="ieee_cis",
            display="IEEE-CIS Fraud Detection (Vesta)",
            source="kaggle",
            target_dir=target_root / "ieee_cis",
            # Original `ieee-fraud-detection` returns 403 for most accounts; we
            # use the public mirror `lixfemso/ieee-fraud-detection` which keeps
            # the exact same files (and uses Apache 2.0 license).
            kaggle_slug="lixfemso/ieee-fraud-detection",
            files=[
                ("train_transaction.csv", "train_transaction.csv"),
                ("train_identity.csv",    "train_identity.csv"),
                ("test_transaction.csv",  "test_transaction.csv"),
                ("test_identity.csv",     "test_identity.csv"),
            ],
            size_hint_mb=1300,
            description="Main training set; richest feature set, V* rule-engine signals.",
        ),
        DatasetSpec(
            key="ulb",
            display="ULB Credit Card Fraud (POC)",
            source="kaggle",
            target_dir=target_root / "ulb",
            kaggle_slug="mlg-ulb/creditcardfraud",
            files=[("creditcard.csv", "creditcard.csv")],
            size_hint_mb=70,
            description="POC dataset. PCA-anonymised; useful for smoke tests only.",
        ),
        DatasetSpec(
            key="sparkov",
            display="Sparkov Synthetic (Streaming stress-test)",
            source="kaggle",
            target_dir=target_root / "sparkov",
            kaggle_slug="kartik2112/fraud-detection",
            files=[("sparkov_generated_data.csv", "sparkov_generated_data.csv")],
            size_hint_mb=120,
            description="Synthetic with demographics + geo. Used for load tests.",
        ),
        DatasetSpec(
            key="paysim",
            display="PaySim (Mobile Money Fraud / GNN)",
            source="kaggle",
            target_dir=target_root / "paysim",
            kaggle_slug="ealaxi/paysim1",
            files=[("PS_20174392719_Eng.csv", "PS_20174392719_Eng.csv")],
            size_hint_mb=470,
            description="Synthetic ledger with nameOrig→nameDest chains, GNN target.",
        ),
    ]


# =============================================================================
# Downloaders
# =============================================================================

def _already_done(spec: DatasetSpec) -> bool:
    """Return True if a successful marker exists AND at least one file is present."""
    marker = spec.target_dir / "_downloaded.marker"
    if not marker.exists():
        return False
    return any(spec.target_dir.glob("**/*.csv"))


def _mark_done(spec: DatasetSpec) -> None:
    spec.target_dir.mkdir(parents=True, exist_ok=True)
    (spec.target_dir / "_downloaded.marker").write_text(
        f"Downloaded for FDS data pipeline.\n"
        f"key={spec.key}\nsource={spec.source}\nsize_hint_mb={spec.size_hint_mb}\n"
    )


def _kaggle_available() -> bool:
    if shutil.which("kaggle"):
        return True
    if Path("~/.kaggle/kaggle.json").expanduser().exists():
        return True
    return bool(os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"))


def _download_kaggle(spec: DatasetSpec) -> None:
    """Download via the Kaggle CLI."""
    if not _kaggle_available():
        raise RuntimeError(
            f"[{spec.key}] Kaggle credentials not found. Either run `kaggle login`, "
            f"place kaggle.json at ~/.kaggle/, or set KAGGLE_USERNAME/KAGGLE_KEY."
        )

    spec.target_dir.mkdir(parents=True, exist_ok=True)
    cmd = ["kaggle", "datasets", "download", spec.kaggle_slug, "-p", str(spec.target_dir), "--unzip"]
    print(f"  → running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)


def _download_github(spec: DatasetSpec) -> None:
    """Download Sparkov via raw GitHub URL (lighter than git-clone)."""
    if spec.github_repo is None:
        raise ValueError(f"[{spec.key}] github_repo missing")

    spec.target_dir.mkdir(parents=True, exist_ok=True)
    owner, repo = spec.github_repo
    subpath = spec.github_subpath or ""

    # Sparkov stores CSVs in `data/` of the repo.  Use API to list & download.
    api = f"https://api.github.com/repos/{owner}/{repo}/contents/{subpath}"
    print(f"  → listing {api}")
    resp = requests.get(api, timeout=30)
    resp.raise_for_status()
    items = resp.json()

    for item in items:
        if item.get("type") != "file" or not item["name"].lower().endswith(".csv"):
            continue
        dest = spec.target_dir / item["name"]
        if dest.exists():
            print(f"  · skip {dest.name} (already present)")
            continue
        _http_download(item["download_url"], dest)


def _http_download(url: str, dest: Path) -> None:
    print(f"  ↓ downloading {url}")
    resp = requests.get(url, stream=True, timeout=60)
    resp.raise_for_status()
    total = int(resp.headers.get("Content-Length", 0))
    with open(dest, "wb") as fh:
        with tqdm(total=total, unit="B", unit_scale=True, desc=dest.name) as bar:
            for chunk in resp.iter_content(chunk_size=64 * 1024):
                fh.write(chunk)
                bar.update(len(chunk))


# =============================================================================
# Orchestration
# =============================================================================

def download(specs: list[DatasetSpec], only: Iterable[str] | None) -> None:
    only_set = set(only) if only else None
    for spec in specs:
        if only_set and spec.key not in only_set:
            continue
        print(f"\n=== {spec.display} ({spec.key}) ===")
        print(f"    {spec.description}")
        print(f"    target: {spec.target_dir}")
        print(f"    size ≈{spec.size_hint_mb} MB")

        if _already_done(spec):
            print(f"  ✓ already present; skipping.")
            continue

        try:
            if spec.source == "kaggle":
                _download_kaggle(spec)
            elif spec.source == "github":
                _download_github(spec)
            else:
                raise NotImplementedError(f"source={spec.source!r} not implemented")
            _mark_done(spec)
            print(f"  ✓ done.")
        except Exception as exc:                                     # noqa: BLE001
            print(f"  ✗ FAILED: {exc}", file=sys.stderr)
            if only_set is not None:                                # explicit request → raise
                raise


def main() -> int:
    p = argparse.ArgumentParser(description="Download FDS datasets.")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--all", action="store_true", help="download every dataset")
    g.add_argument(
        "--datasets",
        help="comma-separated subset of: ieee_cis,ulb,sparkov,paysim",
    )
    args = p.parse_args()

    settings = load_settings()
    raw_dir = settings.raw_dir
    raw_dir.mkdir(parents=True, exist_ok=True)

    specs = _specs(raw_dir)
    only = None if args.all else args.datasets.split(",") if args.datasets else []

    download(specs, only)
    print("\nAll requested datasets processed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())