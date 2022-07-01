#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

CMD_DIR="$DIR/cmd/metadata"

cd "$CMD_DIR"
go build -o metadata main.go
scp "$CMD_DIR"/metadata cc:/pbs/home/f/fjammes
ssh cc "killall /pbs/home/f/fjammes/metadata" || true
ssh cc "time /pbs/home/f/fjammes/metadata"
