"""Ray Serve GPU embedding endpoint for indexing and interactive retrieval."""
from __future__ import annotations

import os

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, Field
from ray import serve

from backend.rag.common.model_integrity import verify_model_files

MODEL_ID = "Qwen3-Embedding-4B-1024-v1"
QUERY_INSTRUCTION = "Given a financial research question, retrieve relevant news articles and annual report passages that answer the question"
app = FastAPI()


class EmbeddingRequest(BaseModel):
    model: str
    input: list[str] = Field(min_length=1, max_length=64)
    input_type: str


@serve.deployment(
    num_replicas=2,
    ray_actor_options={"num_cpus": 4, "num_gpus": 1, "resources": {"annual_report_gpu": 1, "accelerator_type:V100": 0.001}},
    max_ongoing_requests=1,
    max_queued_requests=128,
    health_check_period_s=30,
    health_check_timeout_s=15,
)
@serve.ingress(app)
class QwenEmbeddingDeployment:
    def __init__(self) -> None:
        import torch
        from sentence_transformers import SentenceTransformer

        self.token = os.environ.get("RAY_EMBEDDING_TOKEN", "")
        if not self.token:
            raise RuntimeError("RAY_EMBEDDING_TOKEN is required")
        path = os.environ.get("RAY_EMBEDDING_MODEL_PATH", "/models/modelscope/models/Qwen--Qwen3-Embedding-4B/snapshots/master")
        if not os.path.isdir(path):
            raise RuntimeError(f"Embedding model directory missing: {path}")
        verify_model_files(path)
        if not torch.cuda.is_available():
            raise RuntimeError("Embedding replica requires CUDA")
        self.model = SentenceTransformer(path, device="cuda", model_kwargs={"dtype": torch.float16}, truncate_dim=1024, local_files_only=True)

    def check_health(self) -> None:
        if self.model is None:
            raise RuntimeError("Embedding model is unavailable")

    @app.get("/health")
    def health(self):
        return {"status": "ready", "model": MODEL_ID, "dimension": 1024}

    @app.post("/v1/embeddings")
    def embeddings(self, body: EmbeddingRequest, request: Request):
        if request.headers.get("authorization") != f"Bearer {self.token}":
            raise HTTPException(status_code=401, detail="Unauthorized")
        if body.model != MODEL_ID or body.input_type not in {"query", "document"}:
            raise HTTPException(status_code=400, detail="Invalid model or input type")
        if any(not text.strip() for text in body.input):
            raise HTTPException(status_code=400, detail="Input text cannot be blank")
        texts = [text.strip() for text in body.input]
        if body.input_type == "query":
            texts = [f"Instruct: {QUERY_INSTRUCTION}\nQuery: {text}" for text in texts]
        vectors = self.model.encode(texts, batch_size=min(16, len(texts)), normalize_embeddings=True)
        return {"model": MODEL_ID, "data": [{"index": i, "embedding": row.tolist()} for i, row in enumerate(vectors)]}


embedding_app = QwenEmbeddingDeployment.bind()
