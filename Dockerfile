# Build stage
FROM golang:alpine AS builder

WORKDIR /src

# Install certificates and timezone data for runtime
RUN apk add --no-cache ca-certificates tzdata

# Download dependencies first for layer caching
COPY go.mod go.sum* ./
RUN if [ -f go.sum ]; then go mod download; fi

# Copy full source tree
COPY . .

# Select which entrypoint binary to build (e.g., api, worker, outbox-relay)
ARG TARGET=api
RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/app \
    ./cmd/${TARGET}

# Runtime stage
FROM alpine:3.21 AS runtime

# Create non-root user and group
RUN addgroup -S -g 10001 appgroup && \
    adduser -S -u 10001 -G appgroup appuser

# Copy SSL certificates and timezone info from builder
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /out/app /usr/local/bin/app

USER 10001:10001

ENTRYPOINT ["/usr/local/bin/app"]
