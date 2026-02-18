/*
   Copyright The Soci Snapshotter Authors.

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

/*
   Copyright The containerd Authors.

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

package internal

import (
	"github.com/urfave/cli/v3"
)

const (
	SkipVerifyFlag = "skip-verify"
	PlainHttpFlag  = "plain-http"
	UserFlag       = "user"
	RefreshFlag    = "refresh"
	HostsDirFlag   = "hosts-dir"
	TlsCaCertFlag  = "tlscacert"
	TlsCertFlag    = "tlscert"
	TlsKeyFlag     = "tlskey"
	HttpDumpFlag   = "http-dump"
	HttpTraceFlag  = "http-trace"
)

var RegistryFlags = []cli.Flag{
	&cli.BoolFlag{
		Name:    SkipVerifyFlag,
		Aliases: []string{"k"},
		Usage:   "Skip SSL certificate validation",
	},
	&cli.BoolFlag{
		Name:  PlainHttpFlag,
		Usage: "Allow connections using plain HTTP",
	},
	&cli.StringFlag{
		Name:    UserFlag,
		Aliases: []string{"u"},
		Usage:   "User[:password] Registry user and password",
	},
	&cli.StringFlag{
		Name:  RefreshFlag,
		Usage: "Refresh token for authorization server",
	},
	&cli.StringFlag{
		Name: HostsDirFlag,
		// compatible with "/etc/docker/certs.d"
		Usage: "Custom hosts configuration directory",
	},
	&cli.StringFlag{
		Name:  TlsCaCertFlag,
		Usage: "Path to TLS root CA",
	},
	&cli.StringFlag{
		Name:  TlsCertFlag,
		Usage: "Path to TLS client certificate",
	},
	&cli.StringFlag{
		Name:  TlsKeyFlag,
		Usage: "Path to TLS client key",
	},
	&cli.BoolFlag{
		Name:  HttpDumpFlag,
		Usage: "Dump all HTTP request/responses when interacting with container registry",
	},
	&cli.BoolFlag{
		Name:  HttpTraceFlag,
		Usage: "Enable HTTP tracing for registry interactions",
	},
}
