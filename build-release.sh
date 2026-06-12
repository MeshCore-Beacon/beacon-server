#!/usr/bin/env bash
# Copyright 2026 Beacon Contributors
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# build-release.sh — cross-compile Beacon for all supported platforms.
# Output binaries are written to ./binaries/.

set -euo pipefail

PACKAGE="./cmd/beacon"
OUTPUT_DIR="binaries"
VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")

mkdir -p "$OUTPUT_DIR"

targets=(
    "linux/amd64/"
    "linux/arm64/"
    "linux/arm/7"
    "darwin/amd64/"
    "darwin/arm64/"
    "windows/amd64/"
)

echo "Building Beacon $VERSION"

for target in "${targets[@]}"; do
    GOOS=$(echo "$target" | cut -d/ -f1)
    GOARCH=$(echo "$target" | cut -d/ -f2)
    GOARM=$(echo "$target" | cut -d/ -f3)

    NAME="beacon-${VERSION}-${GOOS}-${GOARCH}"
    if [[ -n "$GOARM" ]]; then
        NAME="${NAME}v${GOARM}"
    fi

    if [[ "$GOOS" == "windows" ]]; then
        NAME="${NAME}.exe"
    fi

    printf "  %-40s" "${GOOS}/${GOARCH}${GOARM:+v$GOARM}"

    env GOOS="$GOOS" GOARCH="$GOARCH" ${GOARM:+GOARM="$GOARM"} \
        go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
        -o "${OUTPUT_DIR}/${NAME}" "$PACKAGE"

    echo "→ ${OUTPUT_DIR}/${NAME}"
done

echo "Done."
