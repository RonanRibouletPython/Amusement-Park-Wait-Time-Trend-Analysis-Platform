# Amusement-Park-Wait-Time-Trend-Analysis-Platform

## Things I learned

### Local Docker

#### Test locally the data ingestion Docker image:
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

#### How to run docker and enter it manually:
docker run -it --rm \
--entrypoint /bin/bash \

### GCP Service Account

#### Lock project ID in:
gcloud config set project amusement-park-wait-time

#### Generate Key for service account:
gcloud iam service-accounts keys create secrets/sa-key.json \
--iam-account=park-pipeline-service-account@amusement-park-wait-time.iam.gserviceaccount.com

#### Create a service account:
gcloud iam service-accounts create sa_name \
    --description="Identity for Amusement Park ETL" \
    --display-name="Park Pipeline SA"

#### Grant Storage Access
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectAdmin"

#### Grant Logging Access
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/logging.logWriter"

#### Grant build permit to any repo
gcloud projects add-iam-policy-binding amusement-park-wait-time \
    --member="serviceAccount:1054759641616@cloudbuild.gserviceaccount.com" \
    --role="roles/artifactregistry.writer"

### GCP Cloud Run

#### Create Artifact Repo
gcloud artifacts repositories create park-repo \
    --repository-format=docker \
    --location=europe-west1 \
    --description="Docker repository for Park Pipeline"

#### List the repos that already exists
gcloud artifacts repositories list --location=europe-west1

#### Grant permission to invoke Cloud Run (Required for the Scheduler to use this SA)
gcloud run jobs add-iam-policy-binding park-ingestion-job \
    --region europe-west1 \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/run.invoker"

#### Build and push the image using yaml file
gcloud builds submit \
    --config cloudbuild.yaml \
    --substitutions=_IMAGE_NAME="europe-west1-docker.pkg.dev/$PROJECT_ID/park-repo/ingestion-job:v2" \
    .

#### Grant Service Account permissions for the Job
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/artifactregistry.reader"

#### Grant permissions to User
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="user:xiaomironan@gmail.com" \
    --role="roles/artifactregistry.reader"

#### Allow User to use service account
gcloud iam service-accounts add-iam-policy-binding \
    park-pipeline-service-account@amusement-park-wait-time.iam.gserviceaccount.com \
    --member="user:xiaomironan@gmail.com" \
    --role="roles/iam.serviceAccountUser"

#### Create or Update the Cloud Run Job
gcloud run jobs create park-ingestion-job \
    --image europe-west1-docker.pkg.dev/$PROJECT_ID/park-repo/ingestion-job:v2 \
    --region europe-west1 \
    --service-account $SA_EMAIL \
    --set-env-vars GCP_PROJECT_ID=$PROJECT_ID \
    --set-env-vars BUCKET_NAME=amusement-park-datalake-v1 \
    --set-env-vars BUCKET_LOCATION=EU \
    --tasks 1 \
    --max-retries 0

#### Test the Job
gcloud run jobs execute park-ingestion-job --region europe-west1

#### Use Cloud Scheduler
gcloud scheduler jobs create http park-ingestion-cron \
    --location europe-west1 \
    --schedule "*/5 * * * *" \
    --uri "https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/park-ingestion-job:run" \
    --http-method POST \
    --oauth-service-account-email $SA_EMAIL

### GIT
Source Control lost my credentials so here is how to set it up again
git config --global user.email newsletterpython13@gmail.com
git config --global user.name RonanRibouletPython