#!/bin/bash

set -euxo pipefail

go build -o metadata main.go
time ./metadata --debug --path ../../itest/case01/ --idx ../../itest/case01/idx
