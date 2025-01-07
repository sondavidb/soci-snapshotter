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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/awslabs/soci-snapshotter/soci"
	"github.com/awslabs/soci-snapshotter/soci/store"
	"github.com/awslabs/soci-snapshotter/util/ioutils"
	"github.com/containerd/containerd/reference"
	"github.com/containerd/containerd/remotes/docker"
	"github.com/containerd/log"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"oras.land/oras-go/v2/content"
	"oras.land/oras-go/v2/errdef"
	"oras.land/oras-go/v2/registry/remote"
)

// Max memory buffer size per process per layer. This should be moved into config.
var maxBufferSize int64 = 1 << 20 // 1 MiB

type Fetcher interface {
	// Fetch fetches the artifact identified by the descriptor. It first checks the local content store
	// and returns a `ReadCloser` from there. Otherwise it fetches from the remote, saves in the local content store
	// and then returns a `ReadCloser`.
	Fetch(ctx context.Context, desc ocispec.Descriptor) ([]io.ReadCloser, bool, error)
	// Store takes in a descriptor and io.Reader and stores it in the local store.
	Store(ctx context.Context, desc ocispec.Descriptor, reader []io.ReadCloser) error
}
type resolverStorage interface {
	content.Resolver
	content.Storage
}

// artifactFetcher is responsible for fetching and storing artifacts in the provided artifact store.
type artifactFetcher struct {
	remoteStore resolverStorage
	localStore  store.BasicStore
	refspec     reference.Spec

	maxPullConcurrency int64
	downloadChunkSize  int64
	smp                *semaphore.Weighted
}

// Constructs a new artifact fetcher
// Takes in the image reference, the local store and the resolver
func newArtifactFetcher(refspec reference.Spec, localStore store.BasicStore, remoteStore resolverStorage, maxPullConcurrency, downloadChunkSize int64) (*artifactFetcher, error) {
	log.G(context.Background()).Debugf("num concurrent processes: %d, chunk size: %d", maxPullConcurrency, downloadChunkSize)
	// maxPullConcurrency is always >=1 so we can always do this
	smp := semaphore.NewWeighted(maxPullConcurrency)
	return &artifactFetcher{
		localStore:         localStore,
		remoteStore:        remoteStore,
		refspec:            refspec,
		maxPullConcurrency: maxPullConcurrency,
		downloadChunkSize:  downloadChunkSize,
		smp:                smp,
	}, nil
}

func newRemoteStore(refspec reference.Spec, client *http.Client) (*remote.Repository, error) {
	repo, err := remote.NewRepository(refspec.Locator)
	if err != nil {
		return nil, fmt.Errorf("cannot create repository %s: %w", refspec.Locator, err)
	}
	repo.Client = client
	repo.PlainHTTP, err = docker.MatchLocalhost(refspec.Hostname())
	if err != nil {
		return nil, fmt.Errorf("cannot create repository %s: %w", refspec.Locator, err)
	}

	return repo, nil
}

// Takes in a descriptor and returns the associated ref to fetch from remote.
// i.e. <hostname>/<repo>@<digest>
func (f *artifactFetcher) constructRef(desc ocispec.Descriptor) string {
	return fmt.Sprintf("%s@%s", f.refspec.Locator, desc.Digest.String())
}

// returns range [lower, upper] based on index, blob size, and max concurrency
func getRange(i, size, chunkSize int64) (lower, upper int64) {
	lower = i * chunkSize
	upper = lower + chunkSize
	if upper > size {
		upper = size
	}
	upper -= 1 // Range is inclusive
	return lower, upper
}

// Fetches the artifact identified by the descriptor.
// It first checks the local store for the artifact.
// If not found, if constructs the ref and fetches it from remote.
// If pulls in a layer are done sequentially, returns an array of size 1 with the io.ReadCloser.
// If the daemon allows for concurrent pulls, it will return the streams in an ordered array
func (f *artifactFetcher) Fetch(ctx context.Context, desc ocispec.Descriptor) ([]io.ReadCloser, bool, error) {
	// Check local store first
	rc, err := f.localStore.Fetch(ctx, desc, 0, 0)
	if err == nil {
		return []io.ReadCloser{rc}, true, nil
	}

	log.G(ctx).WithField("digest", desc.Digest.String()).Infof("fetching artifact from remote")
	if desc.Size == 0 {
		// Digest verification fails is desc.Size == 0
		// Therefore, we try to use the resolver to resolve the descriptor
		// and hopefully get the size.
		// Note that the resolve would fail for size > 4MiB, since that's the limit
		// for the manifest size when using the Docker resolver.
		log.G(ctx).WithField("digest", desc.Digest).Warnf("size of descriptor is 0, trying to resolve it...")
		desc, err = f.resolve(ctx, desc)
		if err != nil {
			return nil, false, fmt.Errorf("size of descriptor is 0; unable to resolve: %w", err)
		}
	}

	var rcs []io.ReadCloser

	log.G(ctx).WithField("digest", desc.Digest.String()).Debugf("layer size: %d", desc.Size)
	// Pull at once if doesn't hit concurrency requirement
	if desc.Size <= f.downloadChunkSize || f.downloadChunkSize <= 0 {
		if desc.Size <= f.downloadChunkSize {
			log.G(ctx).Debugf("layer size (%d) smaller than download chunk size, pulling all at once", desc.Size)
		} else {
			log.G(ctx).Debugf("download_chunk_size is negative, pulling sequentially")
		}
		rc, err = f.remoteStore.Fetch(ctx, desc, 0, 0)
		if err != nil {
			return nil, false, fmt.Errorf("unable to fetch descriptor (%v) from remote store: %w", desc.Digest, err)
		}
		rcs = []io.ReadCloser{rc}
	} else {
		log.G(ctx).Debugf("pulling concurrently %d bytes", desc.Size)

		wg := new(sync.WaitGroup)
		var hasErr atomic.Bool

		numLoops := desc.Size/f.downloadChunkSize + 1
		rcs = make([]io.ReadCloser, numLoops)
		for i := range numLoops {
			wg.Add(1)
			f.smp.Acquire(ctx, 1)

			go func(i int64) {
				defer wg.Done()
				defer f.smp.Release(1)

				lower, upper := getRange(i, desc.Size, f.downloadChunkSize)
				log.G(ctx).WithField("digest", desc.Digest.String()).Debugf("layer lower: %d upper: %v", lower, upper)
				rc, err := f.remoteStore.Fetch(ctx, desc, lower, upper)
				if err != nil {
					log.G(ctx).WithField("digest", desc.Digest.String()).Debugf("process %d returned error: %v", i, err)
					hasErr.Store(true)
					return
				}

				// Maintain order of fetched bytes
				rcs[i] = rc
			}(i)
		}
		wg.Wait()
		err = nil
		if hasErr.Load() {
			err = errors.New("unable to fetch artifact from remote")
		}
	}

	if err != nil {
		return nil, false, fmt.Errorf("unable to fetch descriptor (%v) from remote store: %w", desc.Digest, err)
	}

	return rcs, false, nil
}

func (f *artifactFetcher) resolve(ctx context.Context, desc ocispec.Descriptor) (ocispec.Descriptor, error) {
	ref := f.constructRef(desc)
	desc, err := f.remoteStore.Resolve(ctx, ref)
	if err != nil {
		return desc, fmt.Errorf("unable to resolve ref (%s): %w", ref, err)
	}
	return desc, nil
}

// Store takes in an descriptor and io.Reader and stores it in the local store.
func (f *artifactFetcher) Store(ctx context.Context, desc ocispec.Descriptor, readers []io.ReadCloser) error {
	if f.downloadChunkSize <= 0 {
		err := f.localStore.Push(ctx, desc, readers[0])
		if err != nil {
			return fmt.Errorf("unable to push to local store: %w", err)
		}
	}

	return f.StoreInParallel(ctx, desc, readers)
}

// combineReadClosers concurrently combines the readclosers into a single readcloser,
// consuming the readers in the process.
func combineReadClosers(rcs []io.ReadCloser, size int64) (io.ReadCloser, error) {
	if len(rcs) == 1 {
		return rcs[0], nil
	}

	if size == 0 {
		return nil, errors.New("size of descriptor is zero")
	}

	wg := new(sync.WaitGroup)
	fullContent := make([]byte, size)
	chunkSize := int64(0)
	for i, rc := range rcs {
		if i == 0 {
			// We need to find the expected length of all the readclosers.
			b, _ := io.ReadAll(rc)
			chunkSize = int64(len(b))
			copy(fullContent[0:chunkSize], b)
			continue
		}

		wg.Add(1)

		go func(i int, rc io.ReadCloser) {
			defer wg.Done()
			defer rc.Close()

			b, _ := io.ReadAll(rc)
			lower, upper := getRange(int64(i), size, chunkSize)
			copy(fullContent[lower:upper+1], b)
		}(i, rc)
	}
	wg.Wait()

	return io.NopCloser(bytes.NewReader(fullContent)), nil
}

func FetchSociArtifacts(ctx context.Context, refspec reference.Spec, indexDesc ocispec.Descriptor, localStore store.Store, remoteStore resolverStorage, maxPullConcurrency, downloadChunkSize int64) (*soci.Index, error) {
	fetcher, err := newArtifactFetcher(refspec, localStore, remoteStore, maxPullConcurrency, downloadChunkSize)
	if err != nil {
		return nil, fmt.Errorf("could not create an artifact fetcher: %w", err)
	}

	log.G(ctx).WithField("digest", indexDesc.Digest).Infof("fetching SOCI index from remote registry")

	rcs, local, err := fetcher.Fetch(ctx, indexDesc)
	if err != nil {
		return nil, fmt.Errorf("unable to fetch SOCI index: %w", err)
	}
	r, err := combineReadClosers(rcs, indexDesc.Size)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	tr := ioutils.NewPositionTrackerReader(r)

	var index soci.Index
	err = soci.DecodeIndex(tr, &index)
	if err != nil {
		return nil, fmt.Errorf("cannot deserialize byte data to index: %w", err)
	}

	desc := ocispec.Descriptor{
		Digest: indexDesc.Digest,
		Size:   tr.CurrentPos(),
	}

	// batch will prevent content from being garbage collected in the middle of the following operations
	ctx, batchDone, err := localStore.BatchOpen(ctx)
	if err != nil {
		return nil, err
	}
	defer batchDone(ctx)

	if !local {
		b, err := soci.MarshalIndex(&index)
		if err != nil {
			return nil, err
		}

		err = localStore.Push(ctx, desc, bytes.NewReader(b))
		if err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
			return nil, fmt.Errorf("unable to store index in local store: %w", err)
		}

		err = store.LabelGCRoot(ctx, localStore, desc)
		if err != nil {
			return nil, fmt.Errorf("unable to label index to prevent garbage collection: %w", err)
		}
	}

	eg, ctx := errgroup.WithContext(ctx)
	for i, blob := range index.Blobs {
		blob := blob
		i := i
		eg.Go(func() error {
			rcs, local, err := fetcher.Fetch(ctx, blob)
			if err != nil {
				return fmt.Errorf("cannot fetch artifact: %w", err)
			}
			if local {
				return nil
			}

			if err := fetcher.Store(ctx, blob, rcs); err != nil && !errors.Is(err, errdef.ErrAlreadyExists) {
				return fmt.Errorf("unable to store ztoc in local store: %w", err)
			}

			return store.LabelGCRefContent(ctx, localStore, desc, "ztoc."+strconv.Itoa(i), blob.Digest.String())
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return &index, nil
}

func (f *artifactFetcher) StoreInParallel(ctx context.Context, blob ocispec.Descriptor, rcs []io.ReadCloser) error {
	sha256dir := filepath.Join("/var/lib/soci-snapshotter-grpc/content/blobs", blob.Digest.Algorithm().String())
	if _, err := os.Stat(sha256dir); err != nil {
		err := os.MkdirAll(sha256dir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	blobPath := filepath.Join(sha256dir, blob.Digest.Encoded())
	file, err := os.Create(blobPath)
	if err != nil {
		return err
	}
	file.Truncate(blob.Size)
	defer file.Close()

	wg := new(sync.WaitGroup)
	for i, rc := range rcs {
		rc := rc
		wg.Add(1)

		go func(i int64, rc io.ReadCloser) {
			defer wg.Done()
			lower, upper := getRange(i, blob.Size, f.downloadChunkSize)
			writeInChunks(file, rc, lower, upper)
		}(int64(i), rc)
	}
	wg.Wait()

	return nil
}

func writeInChunks(file *os.File, rc io.ReadCloser, lower, upper int64) {
	defer rc.Close()

	chunkSize := upper - lower + 1
	bufSize := maxBufferSize
	if chunkSize < bufSize {
		bufSize = chunkSize
	}

	var buf []byte
	n := 1
	buf = make([]byte, bufSize)
	for n != 0 {
		n, err := rc.Read(buf)
		if n != 0 {
			file.WriteAt(buf[:n], lower)
			lower += int64(n)
		}
		if err != nil {
			if err != io.EOF {
				log.G(context.Background()).WithField("file", file.Name()).Infof("failed to write to file %v: %v", file.Name(), err)
			}
			break
		}
	}
}
