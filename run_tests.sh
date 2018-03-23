#!/bin/bash

DIR=$(pwd)

cd workloads/small/
"${DIR}/build_debug/bin/harness" small.init small.work small.result "${DIR}/build_debug/bin/main"
