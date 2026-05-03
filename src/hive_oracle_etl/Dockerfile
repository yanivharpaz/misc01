FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src \
    TMPDIR=/tmp/etl

# libsasl2 and krb5 packages are useful for HiveServer2 deployments using SASL/Kerberos.
# gcc is kept only for packages that may need compilation in your environment.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gcc \
        g++ \
        krb5-user \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules \
        libsasl2-modules-gssapi-mit \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt pyproject.toml README.md ./
RUN python -m pip install --upgrade pip setuptools wheel \
    && python -m pip install -r requirements.txt

COPY src ./src
COPY scripts ./scripts
RUN python -m pip install --no-deps -e .

# OpenShift commonly runs containers with an arbitrary UID. Make runtime paths
# writable by root group instead of relying on a fixed user.
RUN mkdir -p /app/logs /tmp/etl \
    && chgrp -R 0 /app /tmp/etl \
    && chmod -R g=u /app /tmp/etl

USER 1001

ENTRYPOINT ["python", "-m", "hive_oracle_etl.main"]
