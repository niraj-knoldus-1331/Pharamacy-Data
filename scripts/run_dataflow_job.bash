#!/usr/bin/env bash

set -e
set -o pipefail
set -u

echo "#######Run the Dataflow Flex Template pipeline"

gcloud dataflow flex-template run "testjob-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location=gs://pharamacy-deda/template/run_template.json \
  --worker-region=us-east4 \
  --worker-machine-type=e2-medium \
  --region=us-east4 \
  --service-account-email=dataflow-run@de-da-ml.iam.gserviceaccount.com \
  --num-workers=1 \
  --parameters gcpProject=de-da-ml \
  --parameters tempLocation=gs://pharamacy-deda/staging_location \
  --parameters stagingLocation=gs://pharamacy-deda/temp_location
