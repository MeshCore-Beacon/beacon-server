# Copyright 2026 Beacon Contributors
# SPDX-License-Identifier: AGPL-3.0-or-later

FROM golang:1.26.10-alpine AS builder
ARG SOURCE_REVISION=unknown
ARG BUILD_VERSION=dev
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w -X main.version=${BUILD_VERSION} -X main.revision=${SOURCE_REVISION}" -o beacon ./cmd/beacon

FROM alpine:3.24
ARG POSTGRES_CLIENT_MAJOR=16
RUN apk add --no-cache ca-certificates tzdata postgresql${POSTGRES_CLIENT_MAJOR}-client
RUN addgroup -g 65532 beacon && adduser -D -u 65532 -G beacon beacon && mkdir /app && chown 65532:65532 /app
WORKDIR /app
COPY --from=builder --chown=65532:65532 /app/beacon .
USER 65532:65532
CMD ["./beacon"]
