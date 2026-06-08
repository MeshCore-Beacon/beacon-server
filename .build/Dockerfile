# Copyright 2026 Beacon Contributors
# SPDX-License-Identifier: agpl

FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o beacon ./cmd/beacon

FROM alpine:3.19
RUN apk add --no-cache ca-certificates tzdata postgresql-client
WORKDIR /app
COPY --from=builder /app/beacon .
COPY db/migrations/ ./migrations/
COPY .build/docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh
ENTRYPOINT ["./docker-entrypoint.sh"]
