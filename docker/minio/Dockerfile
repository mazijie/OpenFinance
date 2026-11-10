# Build the upstream sources without depending on MinIO's image registries.
FROM golang:1.24.9-bookworm AS minio-build
ARG MINIO_REF=master
ENV CGO_ENABLED=0
RUN git clone --depth 1 --branch "${MINIO_REF}" https://github.com/minio/minio.git /src
WORKDIR /src
RUN go build -trimpath -o /out/minio .

FROM golang:1.24.9-bookworm AS mc-build
ARG MC_REF=master
ENV CGO_ENABLED=0
RUN git clone --depth 1 --branch "${MC_REF}" https://github.com/minio/mc.git /src
WORKDIR /src
RUN go build -trimpath -o /out/mc .

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*

FROM runtime AS minio
COPY --from=minio-build /out/minio /usr/local/bin/minio
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]

FROM runtime AS mc
COPY --from=mc-build /out/mc /usr/local/bin/mc
ENTRYPOINT ["mc"]
