// Copyright 2019 Databricks
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/Azure/go-autorest/autorest/azure/cli"

	azblob "github.com/Azure/azure-sdk-for-go/services/storage/mgmt/2019-04-01/storage"
	"github.com/mitchellh/go-homedir"
	ini "gopkg.in/ini.v1"
)

type SASTokenProvider func() (string, error)

type AZBlobConfig struct {
	Endpoint         string
	AccountName      string
	AccountKey       string
	SasToken         SASTokenProvider
	TokenRenewBuffer time.Duration
}

func (config *AZBlobConfig) Init() {
	config.TokenRenewBuffer = 15 * time.Minute
}

type ADLv1Config struct {
	Endpoint   string
	Authorizer autorest.Authorizer
}

func (config *ADLv1Config) Init() {
}

type AzureAuthorizerConfig struct{}

var azbLog = GetLogger("azblob")
var adls1Log = GetLogger("adlv1")

func sptTest(spt *adal.ServicePrincipalToken) (autorest.Authorizer, error) {
	err := spt.EnsureFresh()
	if err != nil {
		return nil, err
	}

	return autorest.NewBearerAuthorizer(spt), nil
}

func tokenToAuthorizer(t *cli.Token) (autorest.Authorizer, error) {
	u, err := url.Parse(t.Authority)
	if err != nil {
		return nil, err
	}

	tenantId := u.Path
	u.Path = ""

	oauth, err := adal.NewOAuthConfig(u.String(), tenantId)
	if err != nil {
		return nil, err
	}

	aToken, err := t.ToADALToken()
	if err != nil {
		return nil, err
	}

	spt, err := adal.NewServicePrincipalTokenFromManualToken(*oauth, t.ClientID, t.Resource,
		aToken)
	if err != nil {
		return nil, err
	}

	return sptTest(spt)
}

func msiToAuthorizer(mc auth.MSIConfig) (autorest.Authorizer, error) {
	// copied from azure/auth/auth.go so we can test this Authorizer
	msiEndpoint, err := adal.GetMSIVMEndpoint()
	if err != nil {
		return nil, err
	}

	var spt *adal.ServicePrincipalToken
	if mc.ClientID == "" {
		spt, err = adal.NewServicePrincipalTokenFromMSI(msiEndpoint, mc.Resource)
	} else {
		spt, err = adal.NewServicePrincipalTokenFromMSIWithUserAssignedID(msiEndpoint, mc.Resource, mc.ClientID)
	}
	if err != nil {
		return nil, err
	}

	return sptTest(spt)
}

func (c AzureAuthorizerConfig) Authorizer() (autorest.Authorizer, error) {
	env, err := auth.GetSettingsFromEnvironment()
	if err != nil {
		return nil, err
	}

	if cred, err := env.GetClientCredentials(); err == nil {
		if authorizer, err := cred.Authorizer(); err == nil {
			return authorizer, err
		}
	}

	if settings, err := auth.GetSettingsFromFile(); err == nil {
		if authorizer, err := settings.ClientCredentialsAuthorizerWithResource(
			auth.Resource); err == nil {
			return authorizer, err
		}
	}

	if env.Values[auth.Resource] == "" {
		env.Values[auth.Resource] = env.Environment.ResourceManagerEndpoint
	}
	if env.Values[auth.ActiveDirectoryEndpoint] == "" {
		env.Values[auth.ActiveDirectoryEndpoint] = env.Environment.ActiveDirectoryEndpoint
	}
	resourceEndpoint := env.Values[auth.Resource]

	accessTokensPath, err := cli.AccessTokensPath()
	if err == nil {
		accessTokens, err := cli.LoadTokens(accessTokensPath)
		if err == nil {
			for _, t := range accessTokens {
				if t.Resource == resourceEndpoint {

					authorizer, err := tokenToAuthorizer(&t)
					if err == nil {
						return authorizer, nil
					}
				}
			}
		}
		adls1Log.Errorf("%v", err)
	}

	return msiToAuthorizer(env.GetMSI())
}

func azureAccountsClient(account string) (azblob.AccountsClient, error) {
	var c azblob.AccountsClient

	profilePath, err := cli.ProfilePath()
	if err != nil {
		return c, err
	}

	profile, err := cli.LoadProfile(profilePath)
	if err != nil {
		return c, err
	}

	var subscriptionId string
	for _, s := range profile.Subscriptions {
		if s.IsDefault {
			subscriptionId = s.ID
			break
		}
	}

	if subscriptionId == "" {
		return c, fmt.Errorf("Unable to find default azure subscription id")
	}

	c = azblob.NewAccountsClient(subscriptionId)
	authorizer, err := AzureAuthorizerConfig{}.Authorizer()
	if err != nil {
		return c, err
	}

	c.BaseClient.Authorizer = authorizer
	return c, nil
}

func azureFindAccount(client azblob.AccountsClient, account string) (string, string, error) {
	accountsRes, err := client.List(context.TODO())
	if err != nil {
		return "", "", err
	}

	for _, acc := range *accountsRes.Value {
		if *acc.Name == account {
			// /subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/...
			parts := strings.SplitN(*acc.ID, "/", 6)
			if len(parts) != 6 {
				return "", "", fmt.Errorf("Malformed account id: %v", *acc.ID)
			}
			return *acc.PrimaryEndpoints.Blob, parts[4], nil
		}
	}

	return "", "", fmt.Errorf("Azure account not found: %v", account)
}

func AzureBlobConfig(endpoint string) (config AZBlobConfig, err error) {
	account := os.Getenv("AZURE_STORAGE_ACCOUNT")
	key := os.Getenv("AZURE_STORAGE_KEY")
	configDir := os.Getenv("AZURE_CONFIG_DIR")

	if account == "" || key == "" {
		if configDir == "" {
			configDir, _ = homedir.Expand("~/.azure")
		}
		if config, err := ini.Load(configDir + "/config"); err == nil {
			if sect, err := config.GetSection("storage"); err == nil {
				if account == "" {
					if k, err := sect.GetKey("account"); err == nil {
						account = k.Value()
						azbLog.Debugf("Using azure account: %v", account)
					}
				}
				if key == "" {
					if k, err := sect.GetKey("key"); err == nil {
						key = k.Value()
					}
				}
			}
		}
	}
	// at this point I have to have the account
	if account == "" {
		err = fmt.Errorf("Missing account: configure via AZURE_STORAGE_ACCOUNT "+
			"or %v/config", configDir)
		return
	}

	if endpoint == "" || key == "" {
		var client azblob.AccountsClient
		client, err = azureAccountsClient(account)
		if err != nil {
			return
		}

		var resourceGroup string
		endpoint, resourceGroup, err = azureFindAccount(client, account)
		if err != nil {
			endpoint = "https://" + account + ".blob." +
				azure.PublicCloud.StorageEndpointSuffix
			azbLog.Debugf("Unable to detect endpoint for account %v, using %v",
				account, endpoint)

			if key == "" {
				err = fmt.Errorf("Missing key: configure via AZURE_STORAGE_KEY "+
					"or %v/config", configDir)
				return
			}
		}
		azbLog.Debugf("Using detected account endpoint: %v", endpoint)

		if key == "" {
			var keysRes azblob.AccountListKeysResult
			keysRes, err = client.ListKeys(context.TODO(), resourceGroup, account)
			if err != nil || len(*keysRes.Keys) == 0 {
				err = fmt.Errorf("Missing key: configure via AZURE_STORAGE_KEY "+
					"or %v/config", configDir)
				return
			}

			// prefer full permission keys
			for _, k := range *keysRes.Keys {
				if k.Permissions == azblob.Full {
					key = *k.Value
					break
				}
			}
			// if not just take the first one
			key = *(*keysRes.Keys)[0].Value
		}
	}

	config.Init()
	config.Endpoint = endpoint
	config.AccountName = account
	config.AccountKey = key

	return
}
