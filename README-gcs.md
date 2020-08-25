# Google Cloud Storage (GCS)


## Prerequisite

Service Account credentials or user authentication. Ensure that either the service account or user has the proper permissions to the Bucket / Object under GCS.

To have a successful mount, we require users to have object listing (`storage.objects.list`) permission to the bucket.

### Service Account credentials

Create a service account credentials (https://cloud.google.com/iam/docs/creating-managing-service-accounts) and generate the JSON credentials file.

### User Authentication and `gcloud` Default Authentication
User can authenticate to gcloud's default environment by first installing cloud sdk (https://cloud.google.com/sdk/) and running `gcloud auth application-default login` command.


## Using Goofys for GCS

### With service account credentials file
```
GOOGLE_APPLICATION_CREDENTIALS="/path/to/creds.json" goofys gs://[BUCKET] /path/to/mount
```

### With user authentication (`gcloud auth application-default login`)

```
goofys gs://[BUCKET] [MOUNT DIRECTORY]
```