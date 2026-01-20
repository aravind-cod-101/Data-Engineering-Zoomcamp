FROM python:3.13.11-slim

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

COPY pyproject.toml uv.lock .python-version ./


RUN uv sync --locked

COPY ingest-pipeline.py .

ENTRYPOINT ["python", "ingest-pipeline.py"]

