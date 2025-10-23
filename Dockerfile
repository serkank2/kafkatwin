# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make ca-certificates

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application with version info
ARG VERSION=2.0.0
ARG BUILD_TIME
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -a -installsuffix cgo \
    -ldflags="-w -s -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" \
    -o kafkatwin ./cmd/proxy

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata wget

# Create non-root user
RUN addgroup -g 1000 kafkatwin && \
    adduser -D -u 1000 -G kafkatwin kafkatwin

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/kafkatwin .

# Copy web assets for Admin UI
COPY --from=builder /build/web /app/web

# Copy example configuration
COPY config.yaml /app/config.example.yaml

# Create directories for logs and data
RUN mkdir -p /var/log/kafkatwin /app/data && \
    chown -R kafkatwin:kafkatwin /app /var/log/kafkatwin

# Switch to non-root user
USER kafkatwin

# Expose ports
# 9092: Kafka proxy port
# 8080: Health check port
# 8090: Admin API & Web UI port
# 9090: Prometheus metrics port
EXPOSE 9092 8080 8090 9090

# Set environment variables with defaults
ENV LOG_LEVEL=INFO \
    PROXY_PORT=9092 \
    PROXY_LISTEN_ADDRESS=0.0.0.0

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run the application
ENTRYPOINT ["/app/kafkatwin"]
CMD ["-config", "/app/config.yaml"]
