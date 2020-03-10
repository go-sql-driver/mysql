#!/bin/bash
set -e 
dist_list=$(go tool dist list)

for dist in ${dist_list}; do
    GOOS=$(echo ${dist} | cut  -d "/" -f 1)
    GOARCH=$(echo ${dist} | cut -d "/" -f 2)
    echo "Building  ${GOOS}/${GOARCH}"
    GOOS=${GOOS} GOARCH=${GOARCH} go build  -o /dev/null
 done
