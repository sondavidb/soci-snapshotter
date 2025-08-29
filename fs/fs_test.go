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

/*
   Copyright 2019 The Go Authors. All rights reserved.
   Use of this source code is governed by a BSD-style
   license that can be found in the NOTICE.md file.
*/

package fs

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/awslabs/soci-snapshotter/fs/layer"
	"github.com/awslabs/soci-snapshotter/fs/remote"
	"github.com/awslabs/soci-snapshotter/fs/source"
	"github.com/awslabs/soci-snapshotter/idtools"
	"github.com/awslabs/soci-snapshotter/snapshot"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
	fusefs "github.com/hanwen/go-fuse/v2/fs"
	digest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func TestConfigTypeChecking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Order matters here — if the index of fsTypes and cfgTypes are the same,
	// the tests should not return ErrWrongFSConfig. If it is not the same,
	// it should return ErrWrongFSConfig.
	fsTypes := []snapshot.FileSystem{&remoteFS{}, &localFS{}, &parallelFS{}}
	cfgTypes := []any{
		&snapshot.RemoteCfg{GetSources: emptyGetSources()},
		&snapshot.LocalCfg{GetSources: emptyGetSources()},
		&snapshot.ParallelCfg{GetSources: emptyGetSources()},
	}

	for i, fs := range fsTypes {
		for j, cfg := range cfgTypes {
			t.Run(fmt.Sprintf("run %s FS with %s", fs.Type(), reflect.TypeOf(cfg)), func(t *testing.T) {
				// t.Parallel()
				// To avoid segfault
				err := fs.Mount(ctx, "", cfg)
				if errors.Is(err, snapshot.ErrWrongFSConfig) {
					if i == j {
						t.Fatalf("expected correct config to be passed but got %v", err)
					}
				} else {
					if i != j {
						t.Fatalf("expected %v but got %v", snapshot.ErrWrongFSConfig, err)
					}
				}
			})

		}
	}
}

func emptyGetSources() func(source.GetSources) ([]source.Source, error) {
	return func(source.GetSources) ([]source.Source, error) {
		return []source.Source{}, nil
	}
}

func TestCheck(t *testing.T) {

	bl := &breakableLayer{}
	remoteFs := &remoteFS{
		layer: map[string]layer.Layer{
			"test": bl,
		},
		getSources: source.FromDefaultLabels(func(imgRefSpec reference.Spec) (hosts []docker.RegistryHost, _ error) {
			return docker.ConfigureDefaultRegistries(docker.WithPlainHTTP(docker.MatchLocalhost))(imgRefSpec.Hostname())
		}),
	}
	localFs := &localFS{}
	parallelFs := &parallelFS{}
	filesystems := []snapshot.FileSystem{remoteFs, localFs, parallelFs}

	for _, fs := range filesystems {
		// remoteFS should succeed; rest should fail
		bl.success = true
		if err := remoteFs.Check(context.TODO(), "test", nil); err != nil && fs.Type() == snapshot.RemoteFS {
			t.Errorf("connection failed; wanted to succeed: %v", err)
		}

		// all should fail
		bl.success = false
		for _, fs := range filesystems {
			if err := fs.Check(context.TODO(), "test", nil); err == nil {
				t.Errorf("connection succeeded; wanted to fail")
			}
		}
	}

}

type breakableLayer struct {
	success bool
}

func (l *breakableLayer) Info() layer.Info {
	return layer.Info{
		Size: 1,
	}
}
func (l *breakableLayer) DisableXAttrs() bool { return false }
func (l *breakableLayer) RootNode(uint32, idtools.IDMap) (fusefs.InodeEmbedder, error) {
	return nil, nil
}
func (l *breakableLayer) Verify(tocDigest digest.Digest) error { return nil }
func (l *breakableLayer) SkipVerify()                          {}
func (l *breakableLayer) ReadAt([]byte, int64, ...remote.Option) (int, error) {
	return 0, fmt.Errorf("fail")
}
func (l *breakableLayer) BackgroundFetch() error { return fmt.Errorf("fail") }
func (l *breakableLayer) Check() error {
	if !l.success {
		return fmt.Errorf("failed")
	}
	return nil
}
func (l *breakableLayer) Refresh(ctx context.Context, hosts []docker.RegistryHost, refspec reference.Spec, desc ocispec.Descriptor) error {
	if !l.success {
		return fmt.Errorf("failed")
	}
	return nil
}
func (l *breakableLayer) Done() {}
