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
COPY services/collector /build/service
RUN pip install --no-cache-dir /build/service && rm -rf /build/service
COPY configs ./configs
USER 10001:10001
CMD ["data-horizon-collector"]
