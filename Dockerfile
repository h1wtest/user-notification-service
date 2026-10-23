FROM golang:1.26.10-alpine3.23@sha256:0525c952defebc693cdaf62cdf43afa32be5a7f377e99260642ea19c240c708e AS builder
ARG TARGET=profile
WORKDIR /build
COPY platform-contracts ./platform-contracts
COPY user-notification-service ./user-notification-service
WORKDIR /build/user-notification-service
RUN case "$TARGET" in profile|notification|worker|admin) ;; *) exit 2 ;; esac && CGO_ENABLED=0 GOOS=linux go build -mod=readonly -trimpath -ldflags="-w -s" -o /build/service ./cmd/${TARGET}

FROM scratch AS runner
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
WORKDIR /app
COPY --from=builder /build/service /app/service
USER 10001:10001
EXPOSE 8002 8003 8004 8005
ENTRYPOINT ["/app/service"]
