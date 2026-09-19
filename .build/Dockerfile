# Copyright 2026 Beacon Contributors
# SPDX-License-Identifier: AGPL-3.0-or-later

FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o beacon ./cmd/beacon

FROM alpine:3.19
ARG POSTGRES_CLIENT_MAJOR=16
RUN apk add --no-cache ca-certificates tzdata postgresql${POSTGRES_CLIENT_MAJOR}-client
WORKDIR /app
COPY --from=builder /app/beacon .
CMD ["./beacon"]
