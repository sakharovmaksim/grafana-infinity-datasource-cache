# The build stage runs on the builder's platform and cross-compiles: no emulation for multi-arch images.
FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine3.24 AS build
ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev
ARG REVISION=unknown
ENV CGO_ENABLED=0 GOTOOLCHAIN=local GOFLAGS=-mod=readonly
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION} -X main.revision=${REVISION}" \
    -o /out/grafana-infinity-datasource-cache ./cmd/grafana-infinity-datasource-cache

# Includes CA certificates and runs as 65532.
FROM gcr.io/distroless/static-debian13:nonroot
COPY --from=build /out/grafana-infinity-datasource-cache /usr/local/bin/grafana-infinity-datasource-cache
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/grafana-infinity-datasource-cache"]
