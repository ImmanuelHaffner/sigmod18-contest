#!/bin/bash

DIR=$(pwd)

cd workloads
for w in ./*; do
    w=$(basename "${w}")
    echo "Testing ${w} workload"
    cd "${w}"
    "${DIR}/build_debug/bin/harness" *.init *.work *.result "${DIR}/build_debug/bin/main"
done
