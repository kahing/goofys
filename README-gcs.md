# Google Cloud Storage (GCS)


## Prerequisite

Service Account credentials or user authentication. Ensure that either the service account or user has the proper permissions to the Bucket / Object under GCS.

For example, a read-only access should be granted the following permissions
```
storage.buckets.get
storage.objects.get
storage.objects.list
````

### Service Account credentials

Create a service account credentials (https://cloud.google.com/iam/docs/creating-managing-service-accounts) and generate the JSON credentials file

### User Authentication
User can authenticate to gcloud's default environment by first installing cloud sdk (https://cloud.google.com/sdk/) and running `gcloud auth application-default login` command


## Using Goofys

Build goofys
1. `cd` to Goofys repository
2. Run `make build` to build the goofys binary

### With service account credentials file
```
GOOGLE_APPLICATION_CREDENTIALS="PATH TO CREDENTIALS FILE"  $GOPATH/src/github.com/kahing/goofys/goofys --debug_cloud -f gs://[BUCKET] [MOUNT DIRECTORY]
```

### With `gcloud` Default Authentication

```
$GOPATH/src/github.com/kahing/goofys/goofys --debug_cloud -f gs://[BUCKET] [MOUNT DIRECTORY]
```