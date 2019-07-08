# Azure Blob Storage

```ShellSession
$ cat ~/.azure/config
[storage]
account = "myblobstorage"
key = "MY-STORAGE-KEY"
$ $GOPATH/bin/goofys wasb://container <mountpoint>
$ $GOPATH/bin/goofys wasb://container:prefix <mountpoint> # if you only want to mount objects under a prefix
```

Users can also configure credentials via `AZURE_STORAGE_ACCOUNT` and
`AZURE_STORAGE_KEY` environment variables. See [Azure CLI configuration](https://docs.microsoft.com/en-us/cli/azure/azure-cli-configuration?view=azure-cli-latest#cli-configuration-values-and-environment-variables) for details. Goofys does not support `connection_string` or `sas_token` yet.

Goofys also accepts full `wasb` URIs:
```ShellSession
$ $GOPATH/bin/goofys wasb://container@myaccount.blob.core.windows.net <mountpoint>
$ $GOPATH/bin/goofys wasb://container@myaccount.blob.core.windows.net/prefix <mountpoint>
```

In this case account configuration in `~/.azure/config` or `AZURE_STORAGE_ACCOUNT` can be omitted. Alternatively, `--endpoint` can also be used to specify storage account:

```ShellSession
$ $GOPATH/bin/goofys --endpoint https://myaccount.blob.core.windows.net wasb://container <mountpoint>
$ $GOPATH/bin/goofys --endpoint https://myaccount.blob.core.windows.net wasb://container:prefix <mountpoint>
```

Note that if full `wasb` URI is not specified, prefix separator is `:`.

Finally, insteading of specifying storage account access key, goofys
can also use [Azure
CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest)
access tokens:

```ShellSession
$ az login
# list all subscribtions and select the needed one
$ az account list
# select current subscription (get its id from previous step)
$ az account set --subscription <name or id>
$ $GOPATH/bin/goofys wasb://container@myaccount.blob.core.windows.net <mountpoint>
```
