// Copyright 2015 Ka-Hing Cheung
// Copyright 2015 Google Inc. All Rights Reserved.
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
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/codegangsta/cli"
)

var awsFlags map[string]bool

// Set up custom help text for goofys; in particular the usage section.
func onlyAws(flags []cli.Flag, aws bool) (ret []cli.Flag) {
	for _, f := range flags {
		if awsFlags[f.GetName()] == aws {
			ret = append(ret, f)
		}
	}
	return
}

func init() {
	cli.AppHelpTemplate = `NAME:
   {{.Name}} - {{.Usage}}

USAGE:
   {{.Name}} {{if .Flags}}[global options]{{end}} bucket[:prefix] mountpoint
   {{if .Version}}
VERSION:
   {{.Version}}
   {{end}}{{if len .Authors}}
AUTHOR(S):
   {{range .Authors}}{{ . }}{{end}}
   {{end}}{{if .Commands}}
COMMANDS:
   {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
   {{end}}{{end}}{{if .Flags}}
GLOBAL OPTIONS:
   {{range onlyAws .Flags false}}{{.}}
   {{end}}
AWS S3 OPTIONS:
   {{range onlyAws .Flags true}}{{.}}
   {{end}}{{end}}{{if .Copyright }}
COPYRIGHT:
   {{.Copyright}}
   {{end}}
`
}

func NewApp() (app *cli.App) {
	uid, gid := MyUserAndGroup()

	app = &cli.App{
		Name:     "goofys",
		Version:  "0.0.9",
		Usage:    "Mount an S3 bucket locally",
		HideHelp: true,
		Writer:   os.Stderr,
		Flags: []cli.Flag{

			cli.BoolFlag{
				Name:  "help, h",
				Usage: "Print this help text and exit successfully.",
			},

			/////////////////////////
			// File system
			/////////////////////////

			cli.StringSliceFlag{
				Name:  "o",
				Usage: "Additional system-specific mount options. Be careful!",
			},

			cli.IntFlag{
				Name:  "dir-mode",
				Value: 0755,
				Usage: "Permission bits for directories. (default: 0755)",
			},

			cli.IntFlag{
				Name:  "file-mode",
				Value: 0644,
				Usage: "Permission bits for files. (default: 0644)",
			},

			cli.IntFlag{
				Name:  "uid",
				Value: uid,
				Usage: "UID owner of all inodes.",
			},

			cli.IntFlag{
				Name:  "gid",
				Value: gid,
				Usage: "GID owner of all inodes.",
			},

			/////////////////////////
			// S3
			/////////////////////////

			cli.StringFlag{
				Name:  "endpoint",
				Value: "",
				Usage: "The non-AWS endpoint to connect to." +
					" Possible values: http://127.0.0.1:8081/",
			},

			cli.StringFlag{
				Name:  "region",
				Value: "us-east-1",
				Usage: "The region to connect to. Usually this is auto-detected." +
					" Possible values: us-east-1, us-west-1, us-west-2, eu-west-1, " +
					"eu-central-1, ap-southeast-1, ap-southeast-2, ap-northeast-1, " +
					"sa-east-1, cn-north-1",
			},

			cli.StringFlag{
				Name:  "storage-class",
				Value: "STANDARD",
				Usage: "The type of storage to use when writing objects." +
					" Possible values: REDUCED_REDUNDANCY, STANDARD, STANDARD_IA.",
			},

			cli.BoolFlag{
				Name: "use-path-request",
				Usage: "Use a path-style request instead of virtual host-style." +
					" (deprecated, always on)",
				Hidden: true,
			},

			cli.StringFlag{
				Name:  "profile",
				Usage: "Use a named profile from $HOME/.aws/credentials instead of \"default\"",
			},

			cli.BoolFlag{
				Name:  "use-content-type",
				Usage: "Set Content-Type according to file extension and /etc/mime.types (default: off)",
			},

			/// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
			/// See http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
			cli.BoolFlag{
				Name:  "sse",
				Usage: "Enable basic server-side encryption at rest (SSE-S3)in S3 for all writes (default: off)",
			},

			cli.StringFlag{
				Name:  "sse-kms",
				Usage: "Enable KMS encryption (SSE-KMS) for all writes using this particular KMS `key-id`. Leave blank to Use the account's CMK - customer master key (default: off)",
				Value: "",
			},

			/// http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl
			cli.StringFlag{
				Name:  "acl",
				Usage: "The canned ACL to apply to the object. Possible values: private, public-read, public-read-write, authenticated-read, aws-exec-read, bucket-owner-read, bucket-owner-full-control (default: off)",
				Value: "",
			},

			/////////////////////////
			// Tuning
			/////////////////////////

			cli.DurationFlag{
				Name:  "stat-cache-ttl",
				Value: time.Minute,
				Usage: "How long to cache StatObject results and inode attributes.",
			},

			cli.DurationFlag{
				Name:  "type-cache-ttl",
				Value: time.Minute,
				Usage: "How long to cache name -> file/dir mappings in directory " +
					"inodes.",
			},

			/////////////////////////
			// Debugging
			/////////////////////////

			cli.BoolFlag{
				Name:  "debug_fuse",
				Usage: "Enable fuse-related debugging output.",
			},

			cli.BoolFlag{
				Name:  "debug_s3",
				Usage: "Enable S3-related debugging output.",
			},

			cli.BoolFlag{
				Name:  "f",
				Usage: "Run goofys in foreground.",
			},
		},
	}

	var funcMap = template.FuncMap{
		"onlyAws": onlyAws,
		"join":    strings.Join,
	}

	awsFlags = map[string]bool{"region": true, "sse": true, "sse-kms": true, "storage-class": true, "acl": true}

	cli.HelpPrinter = func(w io.Writer, templ string, data interface{}) {
		w = tabwriter.NewWriter(w, 1, 8, 2, ' ', 0)
		var tmplGet = template.Must(template.New("help").Funcs(funcMap).Parse(templ))
		tmplGet.Execute(w, app)
	}

	return
}

type FlagStorage struct {
	// File system
	MountOptions map[string]string
	DirMode      os.FileMode
	FileMode     os.FileMode
	Uid          uint32
	Gid          uint32

	// S3
	Endpoint       string
	Region         string
	RegionSet      bool
	StorageClass   string
	UsePathRequest bool
	Profile        string
	UseContentType bool
	UseSSE         bool
	UseKMS         bool
	KMSKeyID       string
	ACL            string

	// Tuning
	StatCacheTTL time.Duration
	TypeCacheTTL time.Duration

	// Debugging
	DebugFuse  bool
	DebugS3    bool
	Foreground bool
}

func parseOptions(m map[string]string, s string) {
	// NOTE(jacobsa): The man pages don't define how escaping works, and as far
	// as I can tell there is no way to properly escape or quote a comma in the
	// options list for an fstab entry. So put our fingers in our ears and hope
	// that nobody needs a comma.
	for _, p := range strings.Split(s, ",") {
		var name string
		var value string

		// Split on the first equals sign.
		if equalsIndex := strings.IndexByte(p, '='); equalsIndex != -1 {
			name = p[:equalsIndex]
			value = p[equalsIndex+1:]
		} else {
			name = p
		}

		m[name] = value
	}

	return
}

// Add the flags accepted by run to the supplied flag set, returning the
// variables into which the flags will parse.
func PopulateFlags(c *cli.Context) (flags *FlagStorage) {
	flags = &FlagStorage{
		// File system
		MountOptions: make(map[string]string),
		DirMode:      os.FileMode(c.Int("dir-mode")),
		FileMode:     os.FileMode(c.Int("file-mode")),
		Uid:          uint32(c.Int("uid")),
		Gid:          uint32(c.Int("gid")),

		// Tuning,
		StatCacheTTL: c.Duration("stat-cache-ttl"),
		TypeCacheTTL: c.Duration("type-cache-ttl"),

		// S3
		Endpoint:       c.String("endpoint"),
		Region:         c.String("region"),
		RegionSet:      c.IsSet("region"),
		StorageClass:   c.String("storage-class"),
		UsePathRequest: c.Bool("use-path-request"),
		Profile:        c.String("profile"),
		UseContentType: c.Bool("use-content-type"),
		UseSSE:         c.Bool("sse"),
		UseKMS:         c.IsSet("sse-kms"),
		KMSKeyID:       c.String("sse-kms"),
		ACL:            c.String("acl"),

		// Debugging,
		DebugFuse:  c.Bool("debug_fuse"),
		DebugS3:    c.Bool("debug_s3"),
		Foreground: c.Bool("f"),
	}

	// KMS implies SSE
	if flags.UseKMS {
		flags.UseSSE = true
	}

	// Handle the repeated "-o" flag.
	for _, o := range c.StringSlice("o") {
		parseOptions(flags.MountOptions, o)
	}
	return
}

func MassageMountFlags(args []string) (ret []string) {
	if len(args) == 5 && args[3] == "-o" {
		// looks like it's coming from fstab!
		mountOptions := ""
		ret = append(ret, args[0])

		for _, p := range strings.Split(args[4], ",") {
			if strings.HasPrefix(p, "-") {
				ret = append(ret, p)
			} else {
				mountOptions += p
				mountOptions += ","
			}
		}

		if len(mountOptions) != 0 {
			// remove trailing ,
			mountOptions = mountOptions[:len(mountOptions)-1]
			ret = append(ret, "-o")
			ret = append(ret, mountOptions)
		}

		ret = append(ret, args[1])
		ret = append(ret, args[2])
	} else {
		return args
	}

	return
}
