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

package snapshot

import (
	"context"
	"errors"
	"fmt"

	"github.com/awslabs/soci-snapshotter/fs/source"
	"github.com/awslabs/soci-snapshotter/idtools"
	"github.com/containerd/containerd/mount"
	ctdsnapshotters "github.com/containerd/containerd/pkg/snapshotters"
)

var ErrWrongFSConfig error = errors.New("incorrect FS config type")

// FileSystem is a backing filesystem abstraction.
//
// Type() returns the type of filesystem being used
// Mount() tries to mount a remote snapshot to the specified mount point
// directory. If succeed, the mountpoint directory will be treated as a layer
// snapshot. If Mount() fails, the mountpoint directory MUST be cleaned up.
// Check() is called to check the connectibity of the existing layer snapshot
// every time the layer is used by containerd.
// Unmount() is called to unmount a remote snapshot from the specified mount point
// directory.
// IDMapMount() is called to mount a mountpoint with ID-mapping. If the backing FS
// does not support ID-mapping, this should return an error.
type FileSystem interface {
	Type() FSType
	Mount(ctx context.Context, mountpoint string, cfg any) error
	IDMapMount(ctx context.Context, mountpoint, activeLayerID string, idmap idtools.IDMap) (string, error)
	Check(ctx context.Context, mountpoint string, labels map[string]string) error
	Unmount(ctx context.Context, mountpoint string) error
	CleanImage(ctx context.Context, digest string) error
}

type FSType string

var (
	RemoteFS   FSType = "remote"
	LocalFS    FSType = "local"
	ParallelFS FSType = "parallel"
	OtherFS    FSType = "other"
)

type RemoteCfg struct {
	SociIndexDigest     string
	ImageRef            string
	ImageManifestDigest string
	TargetLayerDigest   string
	GetSources          func(source.GetSources) ([]source.Source, error)
}

func NewRemoteCfg(labels map[string]string) (*RemoteCfg, error) {
	var ok bool
	cfg := &RemoteCfg{}
	// It's ok if SOCI index digest doesn't exist
	cfg.SociIndexDigest = labels[source.TargetSociIndexDigestLabel]
	cfg.ImageRef, ok = labels[ctdsnapshotters.TargetRefLabel]
	if !ok {
		return nil, errors.New("unable to get image ref from labels")
	}
	cfg.ImageManifestDigest, ok = labels[ctdsnapshotters.TargetManifestDigestLabel]
	if !ok {
		return nil, errors.New("unable to get image digest from labels")
	}
	cfg.TargetLayerDigest, ok = labels[ctdsnapshotters.TargetLayerDigestLabel]
	if !ok {
		return nil, errors.New("unable to get target layer digest from labels")
	}
	cfg.GetSources = func(src source.GetSources) ([]source.Source, error) {
		return src(labels)
	}

	return cfg, nil
}

type LocalCfg struct {
	ImageRef            string
	ImageManifestDigest string
	TargetLayerDigest   string
	GetSources          func(source.GetSources) ([]source.Source, error)
	Mounts              []mount.Mount
}

func NewLocalCfg(labels map[string]string, mounts []mount.Mount) (*LocalCfg, error) {
	var ok bool
	cfg := &LocalCfg{}
	cfg.ImageRef, ok = labels[ctdsnapshotters.TargetRefLabel]
	if !ok {
		return nil, errors.New("unable to get image ref from labels")
	}
	cfg.ImageManifestDigest, ok = labels[ctdsnapshotters.TargetManifestDigestLabel]
	if !ok {
		return nil, errors.New("unable to get image digest from labels")
	}
	cfg.TargetLayerDigest, ok = labels[ctdsnapshotters.TargetLayerDigestLabel]
	if !ok {
		return nil, errors.New("unable to get target layer digest from labels")
	}
	cfg.GetSources = func(src source.GetSources) ([]source.Source, error) {
		return src(labels)
	}

	return cfg, nil
}

type ParallelCfg struct {
	ImageRef            string
	ImageManifestDigest string
	GetSources          func(source.GetSources) ([]source.Source, error)
}

func NewParallelCfg(labels map[string]string) (*ParallelCfg, error) {
	var ok bool
	cfg := &ParallelCfg{}
	cfg.ImageRef, ok = labels[ctdsnapshotters.TargetRefLabel]
	if !ok {
		return nil, errors.New("unable to get image ref from labels")
	}
	cfg.ImageManifestDigest, ok = labels[ctdsnapshotters.TargetManifestDigestLabel]
	if !ok {
		return nil, errors.New("unable to get image digest from labels")
	}
	cfg.GetSources = func(src source.GetSources) ([]source.Source, error) {
		return src(labels)
	}

	return cfg, nil
}

func ErrWrongConfigFunc(fs FileSystem) error {
	return fmt.Errorf("%w (expected %s type)", ErrWrongFSConfig, fs.Type())
}
