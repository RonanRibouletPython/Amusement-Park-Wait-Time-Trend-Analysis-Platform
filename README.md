# Amusement Park Wait Time Analysis - Engineering Cheatsheet

## Architecture overview
*This pipeline follows an ELT (Extract, Load, Transform) pattern:*
1. Bronze Layer: Python extracts JSON from APIs and loads it into Google Cloud Storage (GCS).
2. Silver Layer: Python triggers BigQuery to transform the raw JSON (via External Tables) into structured Native Tables using SQL.

## Requirements
*The following tools are required to run the pipeline:*

* Cloud Build
* Cloud Run
* Cloud Scheduler
* Cloud Storage
* BigQuery
* Artifact Registry

## Environment & Security Setup
*Before running pipelines, we need to configure the project and set up the Service Account (SA) that acts as the identity for our ETL process.*

### Project Configuration
```bash
# Set the active project
gcloud config set project amusement-park-wait-time
```

### Service Account (SA) Management

#### Create the Identity:
```bash
# Create the service account
gcloud iam service-accounts create sa_name \
    --description="Identity for Amusement Park ETL" \
    --display-name="Park Pipeline SA"

# Generate a Key file (for local development usage)
gcloud iam service-accounts keys create secrets/sa-key.json \
    --iam-account=park-pipeline-service-account@amusement-park-wait-time.iam.gserviceaccount.com
```

#### Grant Permissions (IAM Roles):
```bash
# Define variables
export PROJECT_ID=amusement-park-wait-time
export SA_EMAIL=park-pipeline-service-account@amusement-park-wait-time.iam.gserviceaccount.com

# 1. Allow SA to write to Cloud Storage
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/storage.objectAdmin"

# 2. Allow SA to write logs
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/logging.logWriter"

# 3. Allow SA to read images from Artifact Registry
gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/artifactregistry.reader"

# 4. Allow SA to be invoked by Cloud Run (Required for Scheduler)
gcloud run jobs add-iam-policy-binding park-ingestion-job \
    --region europe-west1 \
    --member="serviceAccount:$SA_EMAIL" \
    --role="roles/run.invoker"
```

#### User Permissions (DevOps):
```bash
# Allow Cloud Build SA to write to Artifact Registry
gcloud projects add-iam-policy-binding amusement-park-wait-time \
    --member="serviceAccount:1054759641616@cloudbuild.gserviceaccount.com" \
    --role="roles/artifactregistry.writer"

# Allow YOUR User to act as the Service Account
gcloud iam service-accounts add-iam-policy-binding $SA_EMAIL \
    --member="user:xiaomironan@gmail.com" \
    --role="roles/iam.serviceAccountUser"
 ```

 ## Local Development (Docker)
 *Testing the ingestion logic locally before deploying.*

 ### Prerequisites:
 ```bash
 # Generate Default Credentials for local Auth
gcloud auth application-default login
```

### Build & Run:
```bash
# 1. Build the local image from specified Dockerfile
docker build -t ingestion-test . -f Dockerfile.dev

# 2. Run container with GCP Credentials mapped
docker run --rm \
  -e GCP_PROJECT_ID=amusement-park-wait-time \
  -e BUCKET_NAME=amusement-park-datalake-v1 \
  -e BUCKET_LOCATION=EU \
  -e GOOGLE_APPLICATION_CREDENTIALS="/app/sa-key.json" \
  ingestion-test
```

### Debugging:
```bash
# Enter the container manually to check file structure/environment
docker run -it --rm --entrypoint /bin/bash ingestion-test
```

## Deployment (Artifact Registry)
*Pushing the code to the cloud.*

### Repository Setup
```bash
# Create the Docker Repository
gcloud artifacts repositories create park-repo \
    --repository-format=docker \
    --location=europe-west1 \
    --description="Docker repository for Park Pipeline"

# List repositories
gcloud artifacts repositories list --location=europe-west1
```

### Build & Push
```bash
# Submit build to Cloud Build (pushes to Artifact Registry)
gcloud builds submit \
    --config cloudbuild.yaml \
    --substitutions=_IMAGE_NAME="europe-west1-docker.pkg.dev/$PROJECT_ID/park-repo/ingestion-job:v2" \
    .
```

## Cloud Infrastructure (Cloud Run & Scheduler)
*Running the ETL in production.*

### Create Cloud Run Job
```bash
gcloud run jobs create park-ingestion-job \
    --image europe-west1-docker.pkg.dev/$PROJECT_ID/park-repo/ingestion-job:v2 \
    --region europe-west1 \
    --service-account $SA_EMAIL \
    --set-env-vars GCP_PROJECT_ID=$PROJECT_ID \
    --set-env-vars BUCKET_NAME=amusement-park-datalake-v1 \
    --set-env-vars BUCKET_LOCATION=EU \
    --tasks 1 \
    --max-retries 0
```

### Update Cloud Run Job
```bash
gcloud run jobs update park-ingestion-job \
    --image europe-west1-docker.pkg.dev/$PROJECT_ID/park-repo/ingestion-job:v2 \
    --region europe-west1 \
    --service-account $SA_EMAIL \
    --set-env-vars GCP_PROJECT_ID=$PROJECT_ID \
    --set-env-vars BUCKET_NAME=amusement-park-datalake-v1 \
    --set-env-vars BUCKET_LOCATION=EU \
    --tasks 1 \
    --max-retries 0
```


### Manual Execution
```bash
gcloud run jobs execute park-ingestion-job --region europe-west1
```

### Automate with Cloud Scheduler
```bash
gcloud scheduler jobs create http park-ingestion-cron \
    --location europe-west1 \
    --schedule "*/5 * * * *" \
    --uri "https://europe-west1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$PROJECT_ID/jobs/park-ingestion-job:run" \
    --http-method POST \
    --oauth-service-account-email $SA_EMAIL
```

## Miscellaneous / Config

### Git Configuration:
```bash
git config --global user.email email@mail.com
git config --global user.name username
```

## Engineering Decisions & Roadmap
*Decisions taken during the project development.*

### 🚀 Scalability & Cost Optimization (Architecture Decisions)
*Decisions taken during the project development.*

#### Current Strategy: Full Refresh (MVP)

Currently, the Silver Layer transformation uses a Full Refresh pattern (CREATE OR REPLACE TABLE).
This strategy is suitable for small datasets, as it ensures that the transformed data is always up-to-date.

* Trigger: Every 5 minutes.
* Action: BigQuery scans all historical JSON files in the Bronze layer and rebuilds the Silver table from scratch.
* Why this was chosen:
    ** Development Speed: If we change the parsing logic (e.g., fixing a timezone bug), the entire history is automatically corrected on the next run.
    ** Simplicity: No need to manage state, watermarks, or deduplication logic during the prototyping phase.

#### The Scaling Challenge

As the Data Lake grows, a Full Refresh strategy will become inefficient:

* 1. Cost: BigQuery charges per Byte Scanned. Scanning 1 year of data every 5 minutes to insert 5 minutes worth of data is financially unsustainable.
* 2. Latency: Rebuilding a massive table takes longer, potentially exceeding the 5-minute cron schedule.

#### Future Roadmap: Incremental Loading
To move this to a production-scale system, the pipeline will be refactored to an *Incremental Strategy*:

* 1. Watermarking: The script will query the MAX(timestamp) from the Silver table.
* 2. Partition Pruning: The Bronze External Table is partitioned by year/month/day. The query will only scan partitions relevant to the new data.
* 3. Merge Logic:
```SQL
MERGE INTO `queue_times_cleaned` T
USING (SELECT * FROM `bronze_layer` WHERE timestamp > [WATERMARK]) S
ON T.id = S.id
WHEN MATCHED THEN UPDATE ...
WHEN NOT MATCHED THEN INSERT ...
```
This change will decouple the cost of the run from the size of the history, ensuring O(1) cost scaling rather than O(N)