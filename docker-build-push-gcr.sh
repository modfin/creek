#!/bin/bash

VERSION=$(date +%Y-%m-%dT%H.%M.%S)-$(git log -1 --pretty=format:"%h")
GCR_IMG=eu.gcr.io/spidercave/prod/creek
COMMIT_MSG=$(git log -1 --pretty=format:"%s" .)
AUTHOR=$(git log -1 --pretty=format:"%an" .)

docker build --no-cache -f Dockerfile.prod \
    --label "CommitMsg=${COMMIT_MSG}" \
    --label "Author=${AUTHOR}" \
    -t ${GCR_IMG}:latest \
    -t ${GCR_IMG}:${VERSION} \
    . || exit 1

docker push ${GCR_IMG}:latest
docker push ${GCR_IMG}:${VERSION}

docker rmi -f ${GCR_IMG}:latest
docker rmi -f ${GCR_IMG}:${VERSION}
