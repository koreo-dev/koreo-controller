FROM python:3.13-slim AS build-env
RUN apt-get update && apt-get -y install git curl

# Install UV
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

COPY pyproject.toml uv.lock README.md /app/

WORKDIR /app

# Use UV to sync production dependencies from lockfile
# This is the native UV way to install from a lockfile
RUN uv sync --frozen --no-dev --no-install-project

# The dependencies are installed in .venv, so we need to copy them
RUN cp -r /app/.venv/lib/python*/site-packages /app/__pypackages__

COPY src/ /app/src

FROM python:3.13-slim
RUN apt-get update && apt-get -y install git

COPY --from=build-env /app/__pypackages__ /app/pkgs
COPY --from=build-env /app/src /app/src

RUN adduser --system --no-create-home --uid 1000 nonroot
USER nonroot

WORKDIR /app

ENV PYTHONPATH=/app/src:/app/pkgs

CMD ["python", "src/koreo_controller.py"]
