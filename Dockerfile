# One image for every verisync application service. docker-compose.yml picks
# the entrypoint per service via `command`.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt requirements-pipeline.txt ./
RUN pip install -r requirements.txt -r requirements-pipeline.txt

COPY decision ./decision
COPY ingestion ./ingestion
COPY worker ./worker
COPY merchant_sim ./merchant_sim
COPY dashboard ./dashboard
COPY lite ./lite
COPY scripts ./scripts
COPY db ./db

CMD ["python", "-c", "print('set a command, see docker-compose.yml')"]
