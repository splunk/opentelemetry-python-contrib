# Use the official Ollama base image
FROM ollama/ollama:latest

# Run ollama pull commands to download the models during the image build process
# The 'ollama serve' is needed for 'ollama pull' to work during build
# We run it in the background, pull models, then kill it using its PID.
RUN ollama serve & \
    OLLAMA_PID=$! && \ 
    sleep 5 && \       
    ollama pull nomic-embed-text:latest && \
    ollama pull llama3.2:latest && \
    kill $OLLAMA_PID   