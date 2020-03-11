#!/bin/bash
set -e 
dist_list=$(go tool dist list)

for dist in ${dist_list}; do
    GOOS=$(echo ${dist} | cut  -d "/" -f 1)
    GOARCH=$(echo ${dist} | cut -d "/" -f 2)
    set +e
    GOOS=${GOOS} GOARCH=${GOARCH} go tool compile -V > /dev/null 2>&1 
    if [[ $? -ne 0 ]]; then
        echo "Compile support for ${GOOS}/${GOARCH} is not provided; skipping"
        continue
    fi
    set -e
    echo "Building  ${GOOS}/${GOARCH}"
    GOOS=${GOOS} GOARCH=${GOARCH} go build  -o /dev/null
 done
