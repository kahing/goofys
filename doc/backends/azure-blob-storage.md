### Goofys with Azure Blob Storage

Goofys is already can work with Azure Blob Storage out of the box.
To achive successful mount of azblob container to your fs, you need to follow next simple steps:

- Get account name and blob container name from your Azure Portal
- Get the Access key for your account from Azure Portal like described [here](https://help.bittitan.com/hc/en-us/articles/115008109327-How-do-I-get-an-access-key-for-Azure-Blob-storage- "here").
- You can put account and key values to the config file like this:

```ini
[storage]
account = "myblobstorage"
key = "TZR0DqSYXTVLJRoWUlHnd80qrMJTUW2nBlv1kfaz6gG13OjZpQtKt6GvpgY8JrYsKAUxbWHTHc4KU4Wps5IhYy=="
```
And put the file here: ~/.azure/config
Or instead of this step you can make sure that your shell has these environment variables set before starting goofys:

```sh
export AZURE_STORAGE_ACCOUNT="yblobstorage"
export AZURE_STORAGE_ACCESS_KEY="ZR0DqSYXTVLJRoWUlHnd80qrMJTUW2nBlv1kfaz6gG13OjZpQtKt6GvpgY8JrYsKAUxbWHTHc4KU4Wps5IhYy=="
```
- Install azure-cli using [this instruction](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli?view=azure-cli-latest "this instruction").
- Use azure-cli to authenticate:

```bash
# login first
az login

# list all subscribtions and select the needed one
az account list

# select current subscription (get its id from previous step)
az account set --subscription <name or id>

# write auth data to our azure folder (can take several minutes)
az ad sp create-for-rbac --sdk-auth > ~/.azure/auth
```
- Add path to auth file to AZURE_AUTH_LOCATION env variable:

``` bash
export AZURE_AUTH_LOCATION="~/.azure/auth"
```
- Now we have all necessary info to start the goofys:

```bash
goofys --storage-backend azure-blob-storage <container-name> path/to/mount
```

Now everything should work.
