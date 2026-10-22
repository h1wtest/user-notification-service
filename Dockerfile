# ==========================================
# Build Stage
# ==========================================
FROM golang:alpine AS builder

WORKDIR /build

# Copy platform-contracts module
COPY platform-contracts ./platform-contracts

# Copy user-notification-service source
COPY user-notification-service ./user-notification-service

WORKDIR /build/user-notification-service

RUN go mod tidy && CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o /build/user-notification-service/bin/app ./cmd/app

# ==========================================
# Runtime Stage (Minimal & Secure Non-Root)
# ==========================================
FROM alpine:3.21 AS runner

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -g 10001 -S appgroup && \
    adduser -u 10001 -S appuser -G appgroup

WORKDIR /app

COPY --from=builder /build/user-notification-service/bin/app /app/app
COPY --from=builder /build/user-notification-service/migrations /app/migrations

USER 10001:10001

EXPOSE 8002

ENTRYPOINT ["/app/app"]
