# =============================================================================
# Ollama with pre-pulled + locally-tuned embedding model
# =============================================================================
# Bakes the qwen3-embedding model into the image so it's available immediately
# on container start — no manual `ollama pull` needed.
#
# We pull the 4B variant (qwen3-embedding:4b, ≈ 2.5 GB) rather than the default
# `latest` tag (which resolves to 8B and forces a CPU/GPU offload on an 8 GB
# card — see ollama/Modelfile for the full reasoning). On top of the raw pull
# we run `ollama create` to register a tuned variant called
# `qwen3-embedding-tuned` with bumped num_ctx / num_batch / num_gpu — see
# Modelfile.
#
# The Bill-text-pipeline points OLLAMA_MODEL_NAME at `qwen3-embedding-tuned`
# (not the raw `qwen3-embedding:4b`) so the params take effect on every embed
# call without per-request overrides.
# =============================================================================

FROM ollama/ollama:latest

# Modelfile is COPYed in so `ollama create` can reference it; its contents
# (FROM qwen3-embedding:4b + tuning PARAMETERs) are baked into the
# `qwen3-embedding-tuned` model definition stored under /root/.ollama.
COPY Modelfile /tmp/Modelfile

# Start Ollama in the background, pull the base model, register the tuned
# variant, then stop the server. Both models persist in /root/.ollama inside
# the image layer — no model data crosses container boundary at runtime.
RUN ollama serve & \
    SERVER_PID=$! && \
    sleep 5 && \
    ollama pull qwen3-embedding:4b && \
    ollama create qwen3-embedding-tuned -f /tmp/Modelfile && \
    rm /tmp/Modelfile && \
    kill $SERVER_PID && \
    wait $SERVER_PID 2>/dev/null || true
