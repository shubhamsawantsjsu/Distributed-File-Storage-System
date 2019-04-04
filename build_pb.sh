#!/bin/bash

python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. ./proto/fileService.proto

