#!/bin/bash
set -e
PROTO_DIR="proto"
OUT_DIR="contracts/event"
mkdir -p $OUT_DIR
protoc --proto_path=$PROTO_DIR \
    --go_out=$OUT_DIR --go_opt=paths=source_relative \
    --go-grpc_out=$OUT_DIR --go-grpc_opt=paths=source_relative \
    $PROTO_DIR/event/v1/event.proto
