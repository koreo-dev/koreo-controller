FROM python:3.13-slim AS build-env
RUN apt-get update && apt-get -y install git
RUN pip install --upgrade pip setuptools wheel
RUN pip install pdm

COPY pyproject.toml pdm.lock README.md /app/

WORKDIR /app

RUN mkdir __pypackages__ && pdm sync --prod --no-editable

COPY src/ /app/src

FROM python:3.13-slim
RUN apt-get update && apt-get -y install git

COPY --from=build-env /app/__pypackages__/3.13/lib /app/pkgs
COPY --from=build-env /app/__pypackages__/3.13/bin /app/bin/
COPY --from=build-env /app/src /app/src

RUN adduser --system --no-create-home --uid 1000 nonroot
USER nonroot

WORKDIR /app

ENV PYTHONPATH=/app/src:/app/pkgs

CMD ["python", "src/koreo_controller.py"]
