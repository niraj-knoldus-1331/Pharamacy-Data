#!/usr/bin/env bash

echo "#######Run the Dataflow Flex Template pipeline"

gcloud dataflow flex-template run "testjob-$(date +%Y%m%d-%H%M%S)" \
  --template-file-gcs-location=gs://pharamacy-deda-poc/templates/run_template.json \
  --worker-region=us-east1 \
  --region=us-east1 \
  --service-account-email=dataflow-sa@de-da-poc.iam.gserviceaccount.com \
  --num-workers=1 \
  --parameters gcpProject=de-da-poc \
  --parameters tempLocation=gs://pharamacy-deda-poc/staging_location \
  --parameters stagingLocation=gs://pharamacy-deda-poc/temp_location
