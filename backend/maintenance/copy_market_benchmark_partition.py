"""Deep-copy a single live Iceberg partition into an isolated benchmark table."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
import re
from urllib.parse import urlparse

from minio import Minio
from minio.commonconfig import CopySource
from pyiceberg.exceptions import NamespaceAlreadyExistsError

from backend.config import settings
from backend.lakehouse.catalog import get_catalog


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", required=True, help="Existing benchmark namespace suffix")
    parser.add_argument("--month", required=True, help="YYYY-MM")
    parser.add_argument("--exchange", required=True, choices=("SH", "SZ", "BJ"))
    parser.add_argument("--provider", default="tencent")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,39}", args.dataset):
        parser.error("dataset must be a lowercase identifier")
    try:
        month = date.fromisoformat(args.month + "-01")
    except ValueError:
        parser.error("month must be YYYY-MM")
    month_number = (month.year - 1970) * 12 + month.month - 1
    catalog = get_catalog()
    source = catalog.load_table(f"{settings.ICEBERG_NAMESPACE}.{settings.ICEBERG_DAILY_BARS_TABLE}")
    snapshot = source.current_snapshot()
    if snapshot is None:
        raise ValueError("Source table has no snapshot")
    manifests = snapshot.manifests(source.io)
    with ThreadPoolExecutor(max_workers=12) as pool:
        groups = list(pool.map(lambda m: m.fetch_manifest_entry(source.io), manifests))
    entries = [entry.data_file for group in groups for entry in group]
    if any(int(item.content) != 0 for item in entries):
        raise ValueError("Source snapshot contains delete files; refusing an incomplete copy")
    files = [item.file_path for item in entries if item.partition[0] == month_number
             and str(item.partition[1]) == args.exchange and str(item.partition[2]) == args.provider]
    if not files:
        raise ValueError("No data files in the requested partition")
    target_id = f"bench_{args.dataset}.daily_bars_sample"
    try:
        catalog.create_namespace((f"bench_{args.dataset}",))
    except NamespaceAlreadyExistsError:
        pass
    target = catalog.create_table(target_id, schema=source.schema(), partition_spec=source.spec())
    location = urlparse(target.location())
    if location.scheme != "s3" or not location.netloc or f"bench_{args.dataset}/" not in location.path:
        raise ValueError("Benchmark table location is not isolated")
    client = Minio(settings.MINIO_ENDPOINT, access_key=settings.MINIO_ACCESS_KEY,
                   secret_key=settings.MINIO_SECRET_KEY, secure=settings.MINIO_SECURE)
    root = location.path.strip("/") + "/data/benchmark-input/"

    def copy(item):
        number, path = item
        source_uri = urlparse(path)
        if source_uri.scheme != "s3" or not source_uri.netloc:
            raise ValueError("Expected an S3 source file")
        destination = f"{root}{number:06d}-{source_uri.path.rsplit('/', 1)[-1]}"
        client.copy_object(location.netloc, destination, CopySource(source_uri.netloc, source_uri.path.lstrip("/")))
        return f"s3://{location.netloc}/{destination}"

    with ThreadPoolExecutor(max_workers=12) as pool:
        copied = list(pool.map(copy, enumerate(files)))
    target.add_files(copied)
    print(json.dumps({"source_snapshot_id": str(snapshot.snapshot_id), "target": f"openfinance.{target_id}",
                      "month": args.month, "exchange": args.exchange, "provider": args.provider,
                      "copied_files": len(copied), "target_snapshot_id": str(target.current_snapshot().snapshot_id)}))


if __name__ == "__main__":
    main()
