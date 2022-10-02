#!/bin/bash

set -x

target_container_id="$1"
tag="${2-"v1"}"
#if [[ $tag != "v5.4" && $tag != "v5.10" ]]; then
#    tag="latest"
#fi
image=${3-"kould/perf"}

winpty docker run \
    --cap-add CAP_SYS_ADMIN \
    --privileged \
    -ti \
    --rm \
    --pid=container:$target_container_id \
    --network=container:$target_container_id \
    $image:$tag
