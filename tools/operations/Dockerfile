ARG PYTHON_IMAGE=python:3.11-slim-bookworm
FROM ${PYTHON_IMAGE} AS runtime
ARG VCS_REF
LABEL org.opencontainers.image.revision=$VCS_REF
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 HOME=/tmp/apphome XDG_CACHE_HOME=/tmp/cache PIP_NO_CACHE_DIR=1
WORKDIR /app
COPY packages/core /build/core
RUN pip install --no-cache-dir /build/core \
    && rm -rf /build/core \
    && groupadd --gid 10001 horizon \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin horizon \
    && mkdir -p /data && chown 10001:10001 /data
COPY services/api /build/api
COPY services/collector /build/collector
COPY services/processor /build/processor
COPY tools/operations /build/operations
RUN pip install --no-cache-dir /build/api /build/collector /build/processor /build/operations \
    && rm -rf /build
COPY configs ./configs
COPY scripts ./scripts
USER 10001:10001
CMD ["data-horizon", "--help"]

FROM runtime AS tests
USER root
RUN pip install --no-cache-dir 'data-horizon-operations[test]==0.2.0'
COPY --chown=10001:10001 tests ./tests
COPY --chown=10001:10001 pyproject.toml conftest.py ./
USER 10001:10001
CMD ["python", "-m", "pytest", "tests/test_integration.py", "--junitxml=/tmp/integration.xml"]
