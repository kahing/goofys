module github.com/kahing/goofys

go 1.13

require (
	github.com/Azure/azure-pipeline-go v0.2.3
	github.com/Azure/azure-sdk-for-go v32.1.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.10.0
	github.com/Azure/go-autorest/autorest v0.11.3
	github.com/Azure/go-autorest/autorest/adal v0.9.1
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.0
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.0
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.0 // indirect
	github.com/aws/aws-sdk-go v1.34.5
	github.com/google/uuid v1.1.1
	github.com/jacobsa/fuse v0.0.0-20200706075950-f8927095af03
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/sevlyar/go-daemon v0.1.5
	github.com/shirou/gopsutil v2.20.7+incompatible
	github.com/sirupsen/logrus v1.6.0
	github.com/urfave/cli v1.22.4
	golang.org/x/sys v0.0.0-20200909081042-eff7692f9009
	gopkg.in/ini.v1 v1.57.0
)

replace github.com/jacobsa/fuse => github.com/kahing/fusego v0.0.0-20200327063725-ca77844c7bcc
