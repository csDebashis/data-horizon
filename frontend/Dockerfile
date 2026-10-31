ARG NODE_IMAGE=node:24-bookworm-slim
FROM ${NODE_IMAGE} AS build
ENV NEXT_TELEMETRY_DISABLED=1
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM ${NODE_IMAGE} AS runtime
ARG VCS_REF
LABEL org.opencontainers.image.revision=$VCS_REF
ENV NODE_ENV=production NEXT_TELEMETRY_DISABLED=1 PORT=8501 HOSTNAME=0.0.0.0 HOME=/tmp
WORKDIR /app
RUN groupadd --gid 10001 horizon \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin horizon \
    && mkdir -p /app/.next/cache && chown -R 10001:10001 /app
COPY --from=build --chown=10001:10001 /app/.next/standalone ./
COPY --from=build --chown=10001:10001 /app/.next/static ./.next/static
COPY --from=build --chown=10001:10001 /app/public ./public
USER 10001:10001
EXPOSE 8501
HEALTHCHECK --interval=15s --timeout=5s --start-period=30s --retries=10 CMD ["node", "-e", "fetch('http://127.0.0.1:8501/health/ready').then(r=>{if(!r.ok)process.exit(1)}).catch(()=>process.exit(1))"]
CMD ["node", "server.js"]
