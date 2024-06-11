#!/usr/bin/env bash

#   Copyright The Soci Snapshotter Authors.

#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at

#       http://www.apache.org/licenses/LICENSE-2.0

#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

set -eux -o pipefail

cur_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
soci_snapshotter_project_root="$(cd -- "$cur_dir"/.. && pwd)"

trivy -v
if [ "$?" -eq 1 ]; then
    echo "trivy not installed on machine"
    exit 1
fi

dockerfile="$(grep "FROM" "${soci_snapshotter_project_root}"/Dockerfile)"
readarray -t dockerfile_output <<<"${dockerfile}"

for line in "${dockerfile_output[@]}"; do
    IFS=' ' read -r -a line_arr <<< "${line}"
    trivy image -s CRITICAL -q "${line_arr[1]}" | grep -q "CRITICAL: 0"
    if [ "$?" -eq 1 ]; then
        echo "Error: critical CVEs found in ${line}. Exiting CI."
        exit 1
    fi
done
