FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DEBIAN_FRONTEND=noninteractive \
    JAVA_HOME=/usr/lib/jvm/default-java

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        default-jre-headless \
        tini \
    && rm -rf /var/lib/apt/lists/*

RUN python -m pip install --upgrade pip \
    && python -m pip install \
        "PyYAML>=6,<7" \
        "fastapi>=0.110,<1" \
        "deltalake>=0.21,<1" \
        "dagster>=1.8,<2" \
        "psycopg[binary]>=3.2,<4" \
        "pyspark>=3.5,<3.6" \
        "delta-spark>=3.3,<4" \
        "pytest>=9,<10"

WORKDIR /workspace

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "--version"]
