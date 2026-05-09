# =============================================================================
# Dockerfile
# Multi-stage Docker build for the Superstore Sales Data Pipeline.
#
# Stage 1 — builder:     Installs all Python dependencies into a virtual env.
# Stage 2 — runtime:     Pipeline execution image (no build tools, no API).
# Stage 3 — api-builder: Extends builder with FastAPI + uvicorn deps.
# Stage 4 — api-runtime: FastAPI query service image.
#
# Multi-stage builds keep final images small by excluding compilers, pip
# cache, and build-time artefacts from the production images.
#
# Build pipeline image:  docker build --target runtime -t sales-pipeline .
# Build API image:       docker build --target api-runtime -t sales-api .
# Run pipeline:          docker run --rm -v $(pwd)/data:/app/data sales-pipeline
# =============================================================================


# ---------------------------------------------------------------------------
# Stage 1: Builder
# Uses the official Python 3.11 slim image to install dependencies.
# 'slim' excludes dev tools and documentation, reducing image size.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS builder

# Set the working directory inside the builder container.
WORKDIR /app

# Copy only the dependency manifest first.
# Docker layer caching means pip install is re-run only when requirements.txt
# changes, not on every source code change.
COPY requirements.txt .

# Create the virtual environment that will be copied into the runtime stage.
RUN python -m venv /opt/venv

# Activate the venv so all subsequent pip commands install into it.
ENV PATH="/opt/venv/bin:$PATH"

# Upgrade pip then install all production dependencies into the venv.
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt


# ---------------------------------------------------------------------------
# Stage 2: Runtime
# A clean Python 3.11 slim image with only the venv and application code.
# This is the pipeline execution image — no build tools, no API framework.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

# Set metadata labels visible via 'docker inspect'.
LABEL maintainer="Deepan Mehta"
LABEL description="Superstore Sales Data Pipeline"
LABEL version="1.2.0"

# Set the working directory for the pipeline.
WORKDIR /app

# Copy the pre-built virtual environment from the builder stage.
# This brings in all installed packages without pip or build tools.
COPY --from=builder /opt/venv /opt/venv

# Add the virtual environment to PATH so Python finds installed packages.
ENV PATH="/opt/venv/bin:$PATH"

# Set PYTHONPATH so 'from src...' imports resolve correctly at runtime.
ENV PYTHONPATH="/app"

# Prevent Python from writing .pyc files to disk inside the container.
ENV PYTHONDONTWRITEBYTECODE=1

# Prevent Python from buffering stdout/stderr.
# Unbuffered output ensures log lines appear in 'docker logs' immediately.
ENV PYTHONUNBUFFERED=1

# Copy configuration files.
COPY config/ ./config/

# Copy the pipeline source modules.
COPY src/ ./src/

# Copy the DAG-style orchestrator entry point.
COPY orchestration/ ./orchestration/

# Create output directories that will be overlaid by volume mounts at runtime.
RUN mkdir -p data/bronze data/silver data/gold database logs

# Default command: run the full ETL pipeline.
CMD ["python", "orchestration/pipeline.py"]


# ---------------------------------------------------------------------------
# Stage 3: API Builder
# Extends the base builder stage with FastAPI and uvicorn dependencies.
# Kept as a separate stage so the pipeline runtime image stays lean.
# ---------------------------------------------------------------------------
FROM builder AS api-builder

# Copy the API-specific requirements file into the builder layer.
COPY requirements-api.txt .

# Install FastAPI, uvicorn, and httpx on top of the existing pipeline venv.
RUN pip install --no-cache-dir -r requirements-api.txt


# ---------------------------------------------------------------------------
# Stage 4: API Runtime
# Clean production image for the FastAPI query service.
# Contains the full venv (pipeline + API deps) and the api/ + src/ modules.
# ---------------------------------------------------------------------------
FROM python:3.11-slim AS api-runtime

# Set metadata labels for the API image.
LABEL maintainer="Deepan Mehta"
LABEL description="Superstore Sales Query API"
LABEL version="1.2.0"

# Set the working directory for the API service.
WORKDIR /app

# Copy the full virtual environment (pipeline + API deps) from api-builder.
COPY --from=api-builder /opt/venv /opt/venv

# Activate the copied virtual environment.
ENV PATH="/opt/venv/bin:$PATH"

# Set PYTHONPATH so 'from api...' and 'from src...' imports resolve at runtime.
ENV PYTHONPATH="/app"

# Suppress .pyc file generation inside the container.
ENV PYTHONDONTWRITEBYTECODE=1

# Ensure log output is flushed immediately — critical for Docker log tailing.
ENV PYTHONUNBUFFERED=1

# Copy configuration files.
COPY config/ ./config/

# Copy the pipeline source modules the API depends on.
COPY src/ ./src/

# Copy the FastAPI application package.
COPY api/ ./api/

# Create the database directory the API's DB_PATH expects at startup.
RUN mkdir -p database

# Expose the uvicorn port.
EXPOSE 8000

# Start the FastAPI application via uvicorn.
CMD ["uvicorn", "api.app:app", "--host", "0.0.0.0", "--port", "8000"]
