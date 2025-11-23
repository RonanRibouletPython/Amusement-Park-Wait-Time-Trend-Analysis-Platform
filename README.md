# Amusement-Park-Wait-Time-Trend-Analysis-Platform

## Things I learned

### GCP

Test locally the data ingestion Docker image:
1. Build the image: docker build -t ingestion-test .
2. Run the container with gcloud credentials:
docker run --rm \
  -e GCP_PROJECT_ID=amusement-park-wait-time \
  -e BUCKET_NAME=amusement-park-datalake-v1 \
  -e BUCKET_LOCATION=EU \
  -e GOOGLE_APPLICATION_CREDENTIALS="/app/sa-key.json" \
  ingestion-test

In order to get this application_default_credentials.json file with credentials we need to run the following command:
gcloud auth application-default login

How to run docker and enter it manually:
docker run -it --rm \
--entrypoint /bin/bash \

Lock project ID in:
gcloud config set project amusement-park-wait-time

Generate Key for service account:
gcloud iam service-accounts keys create secrets/sa-key.json \
--iam-account=park-pipeline-service-account@amusement-park-wait-time.iam.gserviceaccount.com

### GIT
Source Control lost my credentials so here is how to set it up again
git config --global user.email newsletterpython13@gmail.com
git config --global user.name RonanRibouletPython