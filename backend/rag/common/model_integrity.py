"""Pinned file checksums for the deployed Qwen embedding snapshot."""
from __future__ import annotations

import hashlib
from pathlib import Path

MODEL_CHECKSUMS = {
    "config.json": "78d2861cbbfd80eee05839200c5a3b7ed64c789f6c1cab4fbb84cc4eae33eaf5",
    "tokenizer.json": "83cdf8c3a34f68862319cb1810ee7b1e2c0a44e0864ae930194ddb76bb7feb8d",
    "tokenizer_config.json": "2f58f4bbd7bbce15d683f525954ef3a92cd82f5e06415a9c513859bf8ab72436",
    "1_Pooling/config.json": "0f0ed3380602b252fced3fab6d07c76752c32ffca818ccc61afaf17ae8edd96f",
    "model.safetensors.index.json": "9d130c7f24fa1f9a2a7e19fad42c7d6d2d6fea31b180bdf3e8aac1924c26c39a",
    "model-00001-of-00002.safetensors": "e70bfe3c970523fb7ef4eddffed2254ce3f1e7150c3de2af4342de129dd756f8",
    "model-00002-of-00002.safetensors": "ed1b87c8e9eb7e535a1a155e4fd00d9f4dba80e58a6db48a4c9f82cede7079c1",
}


def verify_model_files(path: str, checksums: dict[str, str] = MODEL_CHECKSUMS) -> None:
    root = Path(path)
    for name, expected in checksums.items():
        digest = hashlib.sha256()
        try:
            with (root / name).open("rb") as stream:
                for block in iter(lambda: stream.read(8 * 1024 * 1024), b""):
                    digest.update(block)
        except OSError as exc:
            raise RuntimeError(f"Required model file is unavailable: {name}") from exc
        if digest.hexdigest() != expected:
            raise RuntimeError(f"Model snapshot checksum mismatch: {name}")
