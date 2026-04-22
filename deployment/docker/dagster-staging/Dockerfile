FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONPATH=/workspace/src \
    DAGSTER_HOME=/opt/dagster/dagster_home

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jre-headless \
        tini \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY . /workspace

RUN python -m pip install --upgrade pip \
    && python -m pip install -e .[data-proof,dev-test] \
    && python -m pip install "dagster-webserver>=1.8,<2"

RUN mkdir -p /opt/dagster/dagster_home /ta3000-data/moex-historical

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "--version"]
