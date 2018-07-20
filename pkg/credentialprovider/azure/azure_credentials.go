/*
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package azure

import (
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/Azure/azure-sdk-for-go/arm/containerregistry"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure"
	"github.com/ghodss/yaml"
	"github.com/golang/glog"
	"github.com/spf13/pflag"
	"k8s.io/kubernetes/pkg/cloudprovider/providers/azure/auth"
	"k8s.io/kubernetes/pkg/credentialprovider"
)

var flagConfigFile = pflag.String("azure-container-registry-config", "",
	"Path to the file containing Azure container registry configuration information.")

const dummyRegistryEmail = "name@contoso.com"

// init registers the various means by which credentials may
// be resolved on Azure.
func init() {
	credentialprovider.RegisterCredentialProvider("azure",
		&credentialprovider.CachingDockerConfigProvider{
			Provider: NewACRProvider(flagConfigFile),
			Lifetime: 1 * time.Minute,
		})
}

// RegistriesClient is a testable interface for the ACR client List operation.
type RegistriesClient interface {
	List() (containerregistry.RegistryListResult, error)
}

// NewACRProvider parses the specified configFile and returns a DockerConfigProvider
func NewACRProvider(configFile *string) credentialprovider.DockerConfigProvider {
	return &acrProvider{
		file: configFile,
	}
}

type acrProvider struct {
	file                  *string
	config                *auth.AzureAuthConfig
	environment           *azure.Environment
	registryClient        RegistriesClient
	servicePrincipalToken *adal.ServicePrincipalToken
}

// lazyAcrProvider is a DockerConfigProvider that creates on demand an acrProvider
type lazyAcrProvider struct {
	actualProvider *credentialprovider.CachingDockerConfigProvider
}

var _ credentialprovider.DockerConfigProvider = &lazyAcrProvider{}

// Enabled implements DockerConfigProvider.Enabled for the lazy provider.
// Since we perform no checks/work of our own and actualProvider is only created
// later at image pulling time (if ever), always return true.
func (p *lazyAcrProvider) Enabled() bool {
	return true
}

// LazyProvide implements DockerConfigProvider.LazyProvide. It will be called
// by the client when attempting to pull an image and it will create the actual
// provider only when we actually need it the first time.
func (p *lazyAcrProvider) LazyProvide() *credentialprovider.DockerConfigEntry {
	glog.V(2).Infof("Entering LazyProvide...")
	if p.actualProvider == nil {
		glog.V(2).Infof("Creating acrProvider")
		p.actualProvider = &credentialprovider.CachingDockerConfigProvider{
			Provider: &acrProvider{},
			// Refresh credentials a little earlier than expiration time
			Lifetime: 11*time.Hour + 55*time.Minute,
		}
		if !p.actualProvider.Enabled() {
			return nil
		}
	}
	config := p.actualProvider.Provide()
	for _, value := range config {
		return &value
	}
	glog.Warningf("lazyAcrProvider does not contain any credential")
	return nil
}

// Provide implements DockerConfigProvider.Provide, creating dummy credentials.
// Client code will call Provider.LazyProvide() at image pulling time.
func (p *lazyAcrProvider) Provide() credentialprovider.DockerConfig {
	glog.V(2).Infof("Entering Provide...")
	entry := credentialprovider.DockerConfigEntry{
		Provider: p,
	}
	cfg := credentialprovider.DockerConfig{}
	cfg[""] = entry
	return cfg
}

// ParseConfig returns a parsed configuration for an Azure cloudprovider config file
func parseConfig(configReader io.Reader) (*auth.AzureAuthConfig, error) {
	var config auth.AzureAuthConfig

	if configReader == nil {
		return &config, nil
	}

	configContents, err := ioutil.ReadAll(configReader)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(configContents, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func (a *acrProvider) loadConfig(rdr io.Reader) error {
	var err error
	a.config, err = parseConfig(rdr)
	if err != nil {
		glog.Errorf("Failed to load azure credential file: %v", err)
	}

	a.environment, err = auth.ParseAzureEnvironment(a.config.Cloud)
	if err != nil {
		return err
	}

	return nil
}

func (a *acrProvider) Enabled() bool {
	if a.file == nil || len(*a.file) == 0 {
		glog.V(5).Infof("Azure config unspecified, disabling")
		return false
	}

	f, err := os.Open(*a.file)
	if err != nil {
		glog.Errorf("Failed to load config from file: %s", *a.file)
		return false
	}
	defer f.Close()

	err = a.loadConfig(f)
	if err != nil {
		glog.Errorf("Failed to load config from file: %s", *a.file)
		return false
	}

	return true
}

func (a *acrProvider) Provide() credentialprovider.DockerConfig {
	cfg := credentialprovider.DockerConfig{}

	var err error
	a.servicePrincipalToken, err = auth.GetServicePrincipalToken(a.config, a.environment)
	if err != nil {
		glog.Errorf("Failed to create service principal token: %v", err)
		return cfg
	}

	registryClient := containerregistry.NewRegistriesClient(a.config.SubscriptionID)
	registryClient.BaseURI = a.environment.ResourceManagerEndpoint
	registryClient.Authorizer = autorest.NewBearerAuthorizer(a.servicePrincipalToken)
	a.registryClient = registryClient

	glog.V(4).Infof("listing registries")
	res, err := a.registryClient.List()
	if err != nil {
		glog.Errorf("Failed to list registries: %v", err)
		return cfg
	}

	if len(*res.Value) == 0 {
		glog.V(4).Infof("listing registries: empty registry list...")
	}

	for _, registry := range *res.Value {
		loginServer := getLoginServer(registry)
		glog.V(2).Infof("loginServer:%s", loginServer)
		var cred *credentialprovider.DockerConfigEntry

		if a.config.UseManagedIdentityExtension {
			cred, err = getACRDockerEntryFromARMToken(a, loginServer)
			if err != nil {
				continue
			}
		} else {
			cred = &credentialprovider.DockerConfigEntry{
				Username: a.config.AADClientID,
				Password: a.config.AADClientSecret,
				Email:    dummyRegistryEmail,
			}
			glog.V(2).Infof("Username:%s", a.config.AADClientID)
		}

		cfg[loginServer] = *cred
	}
	return cfg
}

func getLoginServer(registry containerregistry.Registry) string {
	return *(*registry.RegistryProperties).LoginServer
}

func getACRDockerEntryFromARMToken(a *acrProvider, loginServer string) (*credentialprovider.DockerConfigEntry, error) {
	armAccessToken := a.servicePrincipalToken.OAuthToken()

	glog.V(4).Infof("discovering auth redirects for: %s", loginServer)
	directive, err := receiveChallengeFromLoginServer(loginServer)
	if err != nil {
		glog.Errorf("failed to receive challenge: %s", err)
		return nil, err
	}

	glog.V(4).Infof("exchanging an acr refresh_token")
	registryRefreshToken, err := performTokenExchange(
		loginServer, directive, a.config.TenantID, armAccessToken)
	if err != nil {
		glog.Errorf("failed to perform token exchange: %s", err)
		return nil, err
	}

	glog.V(4).Infof("adding ACR docker config entry for: %s", loginServer)
	return &credentialprovider.DockerConfigEntry{
		Username: dockerTokenLoginUsernameGUID,
		Password: registryRefreshToken,
		Email:    dummyRegistryEmail,
	}, nil
}

func (a *acrProvider) LazyProvide() *credentialprovider.DockerConfigEntry {
	return nil
}
