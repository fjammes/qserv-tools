#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

go build metadata.go
scp "$DIR"/metadata cc:/pbs/home/f/fjammes
ssh cc "killall /pbs/home/f/fjammes/metadata" || true
ssh cc "time /pbs/home/f/fjammes/metadata"
