#!/usr/bin/env bash

#   Copyright The containerd Authors.

#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

echo "Install soci check tools"
set -eux -o pipefail

golangci_lint_ver="1.56.2"
curl -sfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b "$(go env GOPATH)/bin" v"${golangci_lint_ver}"
# TODO: Verify script integrity via checksums
go install github.com/kunalkushwaha/ltag@v0.2.4
go install github.com/vbatts/git-validation@v1.2.0

scversion="v0.10.0"
arch=$(uname -m)
wget -qO- "https://github.com/koalaman/shellcheck/releases/download/${scversion?}/shellcheck-${scversion?}.linux.${arch}.tar.xz" | tar -xJv
# TODO: Verify script integrity via checksums
cp "shellcheck-${scversion}/shellcheck" "$(go env GOPATH)"/bin/
