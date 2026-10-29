"""Prune LangGraph checkpoints for old archived research threads."""

from __future__ import annotations

import argparse

from backend.config import settings
from backend.harness.store import HarnessStore


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--days",
        type=int,
        default=settings.HARNESS_CHECKPOINT_RETENTION_DAYS,
    )
    args = parser.parse_args()
    removed = HarnessStore().prune_archived_checkpoints(older_than_days=args.days)
    print(f"Removed checkpoints for {removed} archived threads.")


if __name__ == "__main__":
    main()

