# =============================================================================
# Dockerfile
# Multi-stage Docker build for the Superstore Sales Data Pipeline.
#
# Stage 1 (builder): Installs all Python dependencies into a virtual env.
# Stage 2 (runtime): Copies only the venv and source code — no build tools.
#
# Multi-stage builds keep the final image small by excluding compilers,
# pip cache, and other build-time artefacts from the production image.
#
# Build:  docker build -t sales-pipeline .
# Run:    docker run --rm -v $(pwd)/data:/app/data sales-pipeline
# =============================================================================

# ---------------------------------------------------------------------------
# Stage 1: Builder
# Uses the official Python 3.11 slim image to install dependencies.
# 'slim' excludes dev tools and documentation, reducing image size.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

# Set the working directory inside the builder container.
WORKDIR /app

# Copy only the dependency files first.
# Docker layer caching means 'pip install' is re-run only when these
# files change, not on every source code change.
COPY requirements.txt .

# Install dependencies into a virtual environment inside the builder.
# Using a venv makes it trivial to copy the installed packages to the
# runtime stage without carrying along pip or setuptools.
RUN python -m venv /opt/venv                                          # Create the virtual env
ENV PATH="/opt/venv/bin:$PATH"                                        # Add venv to PATH

RUN pip install --upgrade pip                       && \              # Upgrade pip in the venv
    pip install --no-cache-dir -r requirements.txt                    # Install runtime deps (no cache)


# ---------------------------------------------------------------------------
# Stage 2: Runtime
# A clean Python 3.11 slim image with only the venv and application code.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

# Set metadata labels — visible via 'docker inspect'
LABEL maintainer="Your Name <you@example.com>"
LABEL description="Superstore Sales Data Pipeline"
LABEL version="1.0.0"

# Set the working directory for the application.
WORKDIR /app

# Copy the virtual environment from the builder stage.
# This brings in all installed packages without pip or build tools.
COPY --from=builder /opt/venv /opt/venv

# Add the virtual environment to PATH so Python finds installed packages.
ENV PATH="/opt/venv/bin:$PATH"

# Set PYTHONPATH so 'from src...' imports resolve correctly at runtime.
ENV PYTHONPATH="/app"

# Prevent Python from writing .pyc files to disk inside the container.
# Bytecode caching is unnecessary in a containerised, read-only deployment.
ENV PYTHONDONTWRITEBYTECODE=1

# Prevent Python from buffering stdout/stderr.
# Unbuffered output ensures log lines appear in 'docker logs' immediately.
ENV PYTHONUNBUFFERED=1

# Copy the project source code and configuration into the container.
# Copying in layers lets Docker cache unchanged layers efficiently.
COPY config/      ./config/          # Configuration and schema YAML files
COPY src/         ./src/             # Pipeline source modules
COPY orchestration/ ./orchestration/ # Pipeline orchestrator

# Create the output directories inside the container.
# These are overridden by volume mounts in docker-compose.yml so data
# persists on the host machine between container runs.
RUN mkdir -p data/bronze data/silver data/gold database logs

# Default command: run the full ETL pipeline.
# Override at runtime: docker run ... python -m pytest tests/
CMD ["python", "orchestration/pipeline.py"]
