FROM python:3.13-slim

ARG COORDINATOR_BASE_URL=""
ARG COORDINATOR_API_TOKEN=""
ARG DATABASE_URL=""

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOME=/app \
    COORDINATOR_BASE_URL=${COORDINATOR_BASE_URL} \
    COORDINATOR_API_TOKEN=${COORDINATOR_API_TOKEN} \
    DATABASE_URL=${DATABASE_URL}

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    awscli \
    ca-certificates \
    curl \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

COPY . /app

RUN chmod +x /app/docker-entrypoint.sh

EXPOSE 80 443

ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["server"]
