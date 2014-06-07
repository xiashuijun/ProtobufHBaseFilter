#!/bin/bash

cd $(dirname "$0")

cd src/main/protos

protoc --java_out=../java --descriptor_set_out=../resources/example.desc --proto_path=. example.proto protobuf_filter.proto
