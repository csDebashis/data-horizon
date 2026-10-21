ARG PYTHON_IMAGE=python:3.11-slim-bookworm
FROM eclipse-temurin:17-jre-jammy AS java
FROM ${PYTHON_IMAGE}
ARG VCS_REF
LABEL org.opencontainers.image.revision=$VCS_REF
ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:$PATH \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 HOME=/tmp/apphome XDG_CACHE_HOME=/tmp/cache \
    SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
COPY --from=java /opt/java/openjdk /opt/java/openjdk
WORKDIR /app
COPY packages/core /build/core
COPY services/processor /build/processor
RUN pip install --no-cache-dir /build/core '/build/processor[spark]' \
    && rm -rf /build \
    && groupadd --gid 10001 horizon \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin horizon \
    && mkdir -p /data && chown 10001:10001 /data
COPY docker/spark-dependencies /app/docker/spark-dependencies
COPY scripts/fetch_spark_jars.py /app/scripts/fetch_spark_jars.py
RUN SPARK_LOCAL_IP=127.0.0.1 python /app/scripts/fetch_spark_jars.py
COPY scripts/install_native_codecs.py /app/scripts/install_native_codecs.py
RUN python /app/scripts/install_native_codecs.py
ENV JAVA_TOOL_OPTIONS="-Dorg.xerial.snappy.lib.path=/opt/horizon-native -DZstdNativePath=/opt/horizon-native/libzstd-jni.so"
COPY --chown=10001:10001 configs ./configs
COPY --chown=10001:10001 scripts ./scripts
USER 10001:10001
HEALTHCHECK --interval=30s --timeout=5s --start-period=120s --retries=5 \
    CMD ["python", "scripts/check_processor.py"]
CMD ["data-horizon-processor"]
