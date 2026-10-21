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
COPY services/api /build/service
RUN pip install --no-cache-dir /build/service && rm -rf /build/service
COPY configs ./configs
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=20s --timeout=5s --start-period=30s --retries=5 CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live',timeout=3)"]
CMD ["data-horizon-api"]
