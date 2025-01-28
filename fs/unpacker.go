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

package fs

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/containerd/containerd/archive"
	"github.com/containerd/containerd/archive/compression"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/log"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

type Unpacker interface {
	// Unpack takes care of getting the layer specified by descriptor `desc`,
	// decompressing it, putting it in the directory with the path `mountpoint`
	// and applying the difference to the parent layers if there is any.
	// After that the layer can be mounted as non-remote snapshot.
	Unpack(ctx context.Context, desc ocispec.Descriptor, mountpoint string, mounts []mount.Mount) error
}

type Archive interface {
	// Apply decompresses the compressed stream represented by reader `r` and
	// applies it to the directory `root`.
	Apply(ctx context.Context, root string, r io.Reader, opts ...archive.ApplyOpt) (int64, error)
}

type layerArchive struct {
}

func NewLayerArchive() Archive {
	return &layerArchive{}
}

func (la *layerArchive) Apply(ctx context.Context, root string, r io.Reader, opts ...archive.ApplyOpt) (int64, error) {
	// we use containerd implementation here
	// decompress first and then apply
	decompressReader, err := compression.DecompressStream(r)
	if err != nil {
		return 0, fmt.Errorf("cannot decompress the stream: %w", err)
	}
	defer decompressReader.Close()
	return archive.Apply(ctx, root, decompressReader, opts...)
}

type layerUnpacker struct {
	fetcher Fetcher
	archive Archive
}

func NewLayerUnpacker(fetcher Fetcher, archive Archive) Unpacker {
	return &layerUnpacker{
		fetcher: fetcher,
		archive: archive,
	}
}

func (lu *layerUnpacker) Unpack(ctx context.Context, desc ocispec.Descriptor, mountpoint string, mounts []mount.Mount) error {
	start := time.Now().UnixMilli()
	rcs, local, err := lu.fetcher.Fetch(ctx, desc)
	if err != nil {
		return fmt.Errorf("cannot fetch layer: %w", err)
	}

	if !local {
		endRead := time.Now().UnixMilli()
		log.G(ctx).WithField("digest", desc.Digest.String()).Debugf("new layer size: %d", desc.Size)
		log.G(ctx).WithField("digest", desc.Digest.String()).Debugf("content fetched and read in %d ms", endRead-start)

		err = lu.fetcher.Store(ctx, desc, rcs)
		if err != nil {
			return fmt.Errorf("got error storing: %v", err)
		}

		endStore := time.Now().UnixMilli()
		log.G(ctx).WithField("digest", desc.Digest.String()).Debugf("content stored in %d ms", endStore-start)

		rcs, _, err = lu.fetcher.Fetch(ctx, desc)
		if err != nil {
			return fmt.Errorf("cannot fetch layer after storing: %w", err)
		}
	}
	rc, err := combineReadClosers(rcs, desc.Size)
	if err != nil {
		return err
	}
	defer rc.Close()

	digester := digest.Canonical.Digester()
	rc = io.NopCloser(io.TeeReader(rc, digester.Hash()))

	var parents []string
	if len(mounts) > 0 {
		parents, err = getLayerParents(mounts[0].Options)
	}
	if err != nil {
		return fmt.Errorf("cannot get layer parents: %w", err)
	}
	opts := []archive.ApplyOpt{
		archive.WithConvertWhiteout(archive.OverlayConvertWhiteout),
	}
	if len(parents) > 0 {
		opts = append(opts, archive.WithParents(parents))
	}

	_, err = lu.archive.Apply(ctx, mountpoint, rc, opts...)
	if err != nil {
		return fmt.Errorf("cannot apply layer: %w", err)
	}

	digest := digester.Digest()
	if digest != desc.Digest {
		return fmt.Errorf("digests did not match")
	} else {
		log.G(ctx).Debug("good digest")
	}

	return nil
}

func getLayerParents(options []string) (lower []string, err error) {
	const lowerdirPrefix = "lowerdir="

	for _, o := range options {
		if strings.HasPrefix(o, lowerdirPrefix) {
			lower = strings.Split(strings.TrimPrefix(o, lowerdirPrefix), ":")
		}
	}
	return
}
