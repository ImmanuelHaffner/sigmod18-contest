#!/bin/bash

DIR=$(pwd)

cd workloads/small/
"${DIR}/build_release/bin/harness" small.init small.work small.result "${DIR}/build_release/bin/main"
