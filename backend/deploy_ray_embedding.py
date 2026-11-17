"""Deploy the persistent Ray Serve application for Qwen embeddings."""
from __future__ import annotations

import ray
from ray import serve
from backend.ray_embedding_service import embedding_app


def main() -> None:
    ray.init(address="auto", namespace="serve")
    serve.start(detached=True, http_options={"host": "0.0.0.0", "port": 8002})
    serve.run(embedding_app, name="openfinance-embedding", route_prefix="/")
    print("OpenFinance Qwen embedding service deployed", flush=True)


if __name__ == "__main__":
    main()
