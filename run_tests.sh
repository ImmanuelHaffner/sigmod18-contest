#!/bin/bash

DIR=$(pwd)

cd workloads/small
"${DIR}/build_debug/bin/harness" *.init *.work *.result "${DIR}/build_debug/bin/main"
