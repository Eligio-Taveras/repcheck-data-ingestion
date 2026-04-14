# =============================================================================
# Ollama with pre-pulled embedding model
# =============================================================================
# Bakes the qwen3-embedding model into the image so it's available immediately
# on container start — no manual `ollama pull` needed.
# =============================================================================

FROM ollama/ollama:latest

# Start Ollama in the background, pull the model, then stop the server.
# The model weights persist in /root/.ollama inside the image layer.
RUN ollama serve & \
    SERVER_PID=$! && \
    sleep 5 && \
    ollama pull qwen3-embedding && \
    kill $SERVER_PID && \
    wait $SERVER_PID 2>/dev/null || true
