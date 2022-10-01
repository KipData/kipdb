#!/bin/bash

set -x

target_container_id="$1"
version="$(uname -r)"
version="${version%%-*}"
version="${version%.*}"
tag="v${2-$version}"
if [[ $tag != "v5.4" && $tag != "v5.10" ]]; then
    tag="latest"
fi
image=${3-"perf"}

docker run \
    --cap-add CAP_SYS_ADMIN \
    --privileged \
    -ti \
    --rm \
    --pid=container:$target_container_id \
    --network=container:$target_container_id \
    $image:$tag
