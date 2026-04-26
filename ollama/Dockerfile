# =============================================================================
# Ollama with pre-pulled + locally-tuned bill-text embedding model
# =============================================================================
# Bakes the qwen3-embedding model into the image so it's available immediately
# on container start — no manual `ollama pull` needed.
#
# We pull the 0.6B variant (qwen3-embedding:0.6b, ≈ 0.5 GB) rather than the
# default `latest` tag (which resolves to 8B and forces a CPU/GPU offload on
# an 8 GB card) or the previous 4B baseline (which saturated VRAM at small
# batch sizes). The 0.6B model frees ~5 GB of VRAM on an 8 GB card, lifting
# GPU-saturation batch size from ~10 to ~50 — see ollama/Modelfile for the
# full reasoning.
#
# On top of the raw pull we run `ollama create` to register a tuned variant
# called `bill-text-embedding` (generic, model-agnostic name — the tag stays
# stable as the underlying base is swapped). The tuned variant carries the
# bumped num_ctx / num_batch / num_gpu from the Modelfile.
#
# The bill-text-pipeline points OLLAMA_MODEL_NAME at `bill-text-embedding`
# (not the raw `qwen3-embedding:0.6b`) so the params take effect on every
# embed call without per-request overrides.
# =============================================================================

FROM ollama/ollama:latest

# Modelfile is COPYed in so `ollama create` can reference it; its contents
# (FROM qwen3-embedding:0.6b + tuning PARAMETERs) are baked into the
# `bill-text-embedding` model definition stored under /root/.ollama.
COPY Modelfile /tmp/Modelfile

# Start Ollama in the background, pull the base model, register the tuned
# variant, then stop the server. Both models persist in /root/.ollama inside
# the image layer — no model data crosses container boundary at runtime.
RUN ollama serve & \
    SERVER_PID=$! && \
    sleep 5 && \
    ollama pull qwen3-embedding:0.6b && \
    ollama create bill-text-embedding -f /tmp/Modelfile && \
    rm /tmp/Modelfile && \
    kill $SERVER_PID && \
    wait $SERVER_PID 2>/dev/null || true
