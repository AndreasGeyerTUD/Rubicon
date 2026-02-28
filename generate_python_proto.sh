#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_SRC_DIR="${SCRIPT_DIR}/proto"
PROTO_OUT_DIR="${SCRIPT_DIR}/experiments/proto"

mkdir -p "${PROTO_OUT_DIR}"

protoc \
    --python_out="${PROTO_OUT_DIR}" \
    -I"${PROTO_SRC_DIR}" \
    "${PROTO_SRC_DIR}"/*.proto

# Fix cross-imports: turn "import xxx_pb2" into "from . import xxx_pb2"
# so they resolve correctly inside the proto package.
for f in "${PROTO_OUT_DIR}"/*_pb2.py; do
    sed -i 's/^import \(.*_pb2\)/from . import \1/' "$f"
done

# Make it a proper Python package
touch "${PROTO_OUT_DIR}/__init__.py"

echo "Python protobuf files generated in ${PROTO_OUT_DIR}"