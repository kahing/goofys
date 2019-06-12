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

package internal

import (
	"net/url"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/Azure/go-autorest/autorest/azure/cli"
)

type AzureAuthorizerConfig struct{}

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
