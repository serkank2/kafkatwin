# Build stage
FROM golang:1.21-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o kafkatwin ./cmd/proxy

# Runtime stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 kafkatwin && \
    adduser -D -u 1000 -G kafkatwin kafkatwin

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/kafkatwin .

# Copy example config
COPY configs/example-config.yaml /app/example-config.yaml

# Create directories
RUN mkdir -p /var/log/kafkatwin && \
    chown -R kafkatwin:kafkatwin /app /var/log/kafkatwin

# Switch to non-root user
USER kafkatwin

# Expose ports
EXPOSE 9092 8080 9090

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health/live || exit 1

# Run the application
ENTRYPOINT ["/app/kafkatwin"]
CMD ["-config", "/app/config.yaml"]
