FROM python:3.13-slim AS build-env
RUN apt-get update && apt-get -y install git
RUN pip install --upgrade pip setuptools wheel

COPY pyproject.toml README.md /app/

WORKDIR /app

# Extract dependencies from pyproject.toml and install with pip
# PDM has compatibility issues with Python 3.13 causing segfaults
RUN echo "koreo-core==0.1.17" > requirements.txt && \
    echo "cel-python==0.3.0" >> requirements.txt && \
    echo "kr8s==0.20.7" >> requirements.txt && \
    echo "uvloop==0.21.0" >> requirements.txt && \
    echo "starlette==0.47.2" >> requirements.txt && \
    echo "uvicorn==0.35.0" >> requirements.txt

RUN pip install -r requirements.txt --target __pypackages__

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
