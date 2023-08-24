#!/bin/bash

VERSION=$(date +%Y-%m-%dT%H.%M.%S)-$(git log -1 --pretty=format:"%h")
IMG=modfin/creek
COMMIT_MSG=$(git log -1 --pretty=format:"%s" .)
AUTHOR=$(git log -1 --pretty=format:"%an" .)

docker build --no-cache -f Dockerfile.prod \
    --label "CommitMsg=${COMMIT_MSG}" \
    --label "Author=${AUTHOR}" \
    -t ${IMG}:latest \
    -t ${IMG}:${VERSION} \
    . || exit 1

docker push ${IMG}:latest
docker push ${IMG}:${VERSION}

docker rmi -f ${IMG}:latest
docker rmi -f ${IMG}:${VERSION}