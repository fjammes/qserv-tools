#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

CMD_DIR="$DIR/cmd/ingest"

cd "$CMD_DIR"
go build -o ingest main.go
./ingest --kubeconfig ~/.kube/kubeconfig-k8s-qserv.yaml 
