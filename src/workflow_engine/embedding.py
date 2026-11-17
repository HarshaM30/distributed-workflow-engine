"""Text embedding with fastembed (ONNX Runtime; no PyTorch).

The model is loaded lazily, once per process, on first use. Loading and inference are both
CPU-bound, so they run in a worker thread and the event loop (heartbeats, HTTP) stays live.
"""

from __future__ import annotations

import asyncio
import os
import threading
import warnings
from collections.abc import Sequence
from typing import Any

from workflow_engine.config import Settings
from workflow_engine.db.models import EMBEDDING_DIM
from workflow_engine.logging import get_logger

log = get_logger(__name__)

# huggingface_hub download bars write to stderr and would break the one-JSON-line-per-log rule.
os.environ.setdefault("HF_HUB_DISABLE_PROGRESS_BARS", "1")


class Embedder:
    def __init__(self, model_name: str, cache_dir: str, threads: int, batch_size: int) -> None:
        self.model_name = model_name
        self._cache_dir = cache_dir
        self._threads = threads
        self.batch_size = batch_size
        self._model: Any = None
        self._lock = threading.Lock()

    def _load(self) -> Any:
        with self._lock:
            if self._model is None:
                from fastembed import TextEmbedding

                with warnings.catch_warnings():
                    # fastembed tries to re-enable the progress bars we turned off above.
                    warnings.filterwarnings("ignore", message="Cannot enable progress bars")
                    model = TextEmbedding(
                        self.model_name, cache_dir=self._cache_dir, threads=self._threads
                    )
                dim = len(next(iter(model.embed(["dimension probe"]))))
                if dim != EMBEDDING_DIM:
                    raise RuntimeError(
                        f"{self.model_name} produces {dim}-d vectors; the column is "
                        f"vector({EMBEDDING_DIM})"
                    )
                log.info("embedding_model_loaded", model=self.model_name, dim=dim)
                self._model = model
            return self._model

    def _embed_passages(self, texts: Sequence[str]) -> list[list[float]]:
        model = self._load()
        return [v.tolist() for v in model.passage_embed(list(texts), batch_size=self.batch_size)]

    def _embed_query(self, text: str) -> list[float]:
        model = self._load()
        vector: list[float] = next(iter(model.query_embed(text))).tolist()
        return vector

    async def embed_passages(self, texts: Sequence[str]) -> list[list[float]]:
        return await asyncio.to_thread(self._embed_passages, texts)

    async def embed_query(self, text: str) -> list[float]:
        return await asyncio.to_thread(self._embed_query, text)


_embedder: Embedder | None = None
_embedder_lock = threading.Lock()


def get_embedder(settings: Settings) -> Embedder:
    """The process-wide embedder (the model itself still loads on first use)."""
    global _embedder
    with _embedder_lock:
        if _embedder is None:
            _embedder = Embedder(
                settings.embed_model_name,
                settings.fastembed_cache_dir,
                settings.embed_threads,
                settings.embed_batch_size,
            )
        return _embedder
