#!/bin/bash

set -euxo pipefail

go build metadata.go
scp $PWD/metadata cc:/pbs/home/f/fjammes
ssh cc "killall /pbs/home/f/fjammes/metadata" || true
ssh cc "time /pbs/home/f/fjammes/metadata"
