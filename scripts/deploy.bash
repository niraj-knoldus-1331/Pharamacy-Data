#!/usr/bin/env bash

set -e
set -o pipefail
set -u
cp /jar/Pharamacy-Data.jar /target
ls target/
docker build -t gcr.io/$PROJECT_ID/pharamacy-deda -f Dockerfile .
docker push gcr.io/$PROJECT_ID/pharamacy-deda