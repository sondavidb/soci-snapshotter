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

package store

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/awslabs/soci-snapshotter/util/namedmutex"
	"github.com/containerd/containerd/content"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

var DefaultExpirationTime = 12 * time.Hour

// LabelStore keeps track of the labels for each layer.
// This also works as a defacto digest store,
// where the existence of digests are verified
// by their existence in the labelStore.

// TODO: Probably track by string instead of digest? Though this makes it a little clearer
type labelStore struct {
	mu *namedmutex.NamedMutex
	m  map[digest.Digest]map[string]string
}

func newLabelStore() *labelStore {
	return &labelStore{
		mu: new(namedmutex.NamedMutex),
		m:  map[digest.Digest]map[string]string{},
	}
}

func (ls *labelStore) Load(key digest.Digest) (map[string]string, bool) {
	ls.mu.Lock(key.String())
	defer ls.mu.Unlock(key.String())

	val, ok := ls.m[key]
	return val, ok
}

// Update merges the given label map with the current label map,
// overwriting any values that already exists.
func (ls *labelStore) Update(key digest.Digest, val map[string]string) (map[string]string, error) {
	ls.mu.Lock(key.String())
	defer ls.mu.Unlock(key.String())

	curVal, ok := ls.m[key]
	if !ok {
		ls.Store(key, val)
		return val, nil
	}

	for k, v := range val {
		curVal[k] = v
	}

	ls.m[key] = curVal
	return curVal, nil
}

// Store replaces the previous label map with the current labels map
func (ls *labelStore) Store(key digest.Digest, val map[string]string) {
	ls.mu.Lock(key.String())
	defer ls.mu.Unlock(key.String())

	ls.m[key] = val
}

func (ls *labelStore) Delete(key digest.Digest) {
	ls.mu.Lock(key.String())
	defer ls.mu.Unlock(key.String())

	delete(ls.m, key)
}

func (ls *labelStore) getLabel(dgst digest.Digest, label string) (string, error) {
	labels, ok := ls.Load(dgst)
	if !ok {
		return "", errdefs.ErrNotFound
	}

	retVal, ok := labels[label]
	if !ok {
		return "", errdefs.ErrNotFound
	}

	return retVal, nil
}

func (ls *labelStore) setLabel(dgst digest.Digest, labelKey, labelVal string) error {
	ls.mu.Lock(dgst.String())
	defer ls.mu.Unlock(dgst.String())

	labels, ok := ls.m[dgst]
	if !ok {
		labels = map[string]string{}
	}

	labels[labelKey] = labelVal
	ls.m[dgst] = labels
	return nil
}

func (ls *labelStore) removeLabel(dgst digest.Digest, label string) error {
	ls.mu.Lock(dgst.String())
	defer ls.mu.Unlock(dgst.String())

	labels, ok := ls.m[dgst]
	if !ok {
		return errdefs.ErrNotFound
	}

	_, ok = labels[label]
	if !ok {
		return errdefs.ErrNotFound
	}

	delete(labels, label)
	ls.m[dgst] = labels
	return nil
}

// layerPullStatus keeps track of the current status of
// a layer being fetched from the upstream repo
type layerPullStatus struct {
	expiryTime time.Time
	pulled     bool
	err        error
}

type layerPullMap struct {
	mu *namedmutex.NamedMutex
	m  map[digest.Digest]layerPullStatus
}

func newLayerPullMap() *layerPullMap {
	return &layerPullMap{
		mu: new(namedmutex.NamedMutex),
		m:  map[digest.Digest]layerPullStatus{},
	}
}

func (m *layerPullMap) Load(key digest.Digest) (layerPullStatus, bool) {
	m.mu.Lock(key.String())
	defer m.mu.Unlock(key.String())

	val, ok := m.m[key]
	return val, ok
}

func (m *layerPullMap) Store(key digest.Digest, val layerPullStatus) {
	m.mu.Lock(key.String())
	defer m.mu.Unlock(key.String())

	m.m[key] = val
}

func (m *layerPullMap) Delete(key digest.Digest) {
	m.mu.Lock(key.String())
	defer m.mu.Unlock(key.String())

	delete(m.m, key)
}

// layerUnpackStatus keeps track of the current status of
// a layer being unpacked into a snapshot directory
type layerUnpackStatus struct {
	expiryTime   time.Time
	unpacked     bool
	tempLocation string
	err          error
}

// layerUnpackMap keeps track of each individual chain of snapshots
type layerUnpackMap struct {
	mu *namedmutex.NamedMutex
	m  map[string]layerUnpackStatus
}

func newLayerUnpackMap() *layerUnpackMap {
	return &layerUnpackMap{
		mu: new(namedmutex.NamedMutex),
		m:  map[string]layerUnpackStatus{},
	}
}

func (m *layerUnpackMap) Load(key string) (layerUnpackStatus, bool) {
	m.mu.Lock(key)
	defer m.mu.Unlock(key)

	val, ok := m.m[key]
	return val, ok
}

func (m *layerUnpackMap) Store(key string, val layerUnpackStatus) {
	m.mu.Lock(key)
	defer m.mu.Unlock(key)

	m.m[key] = val
}

func (m *layerUnpackMap) Delete(key string) {
	m.mu.Lock(key)
	defer m.mu.Unlock(key)

	delete(m.m, key)
}

// layerOperationStore is a central location to keep track of
// all layer operations that require us to keep track
// of the state of its operations
type layerOperationStore struct {
	pullMap   *layerPullMap
	unpackMap *layerUnpackMap
}

type RevampStore struct {
	root string
	ls   *labelStore
	los  *layerOperationStore
}

// assert that RevampStore implements Store
var _ Store = (*RevampStore)(nil)

func NewRevampStore(ctx context.Context) *RevampStore {
	return &RevampStore{
		root: DefaultSociContentStorePath,
		ls:   newLabelStore(),
		los: &layerOperationStore{
			pullMap:   newLayerPullMap(),
			unpackMap: newLayerUnpackMap(),
		},
	}
}

// TODO: Implement batching of some sort to stop GC operations on batched calls
func (s *RevampStore) BatchOpen(ctx context.Context) (context.Context, CleanupFunc, error) {
	return ctx, NopCleanup, nil
}

func (s *RevampStore) Exists(ctx context.Context, target ocispec.Descriptor) (bool, error) {
	m, ok := s.ls.Load(target.Digest)
	if !ok || m == nil {
		return false, nil
	}

	return ok, nil
}

func (s *RevampStore) Fetch(ctx context.Context, target ocispec.Descriptor) (io.ReadCloser, error) {
	return io.NopCloser(io.MultiReader()), nil
}

func (s *RevampStore) Push(ctx context.Context, target ocispec.Descriptor, r io.Reader) error {
	return nil
}

func (s *RevampStore) Label(ctx context.Context, target ocispec.Descriptor, name, value string) error {
	info := content.Info{
		Digest: target.Digest,
		Labels: map[string]string{name: value},
	}
	paths := []string{"labels." + name}
	_, err := s.update(ctx, info, paths...)
	if err != nil {
		return err
	}
	return nil
}

// Most of this logic is taken from https://github.com/containerd/containerd/blob/v1.7.25/content/local/store.go
func (s *RevampStore) Info(ctx context.Context, dgst digest.Digest) (content.Info, error) {
	p, err := s.blobPath(dgst)
	if err != nil {
		return content.Info{}, fmt.Errorf("calculating blob info path: %w", err)
	}

	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("content %v: %w", dgst, errdefs.ErrNotFound)
		}

		return content.Info{}, err
	}
	labels, ok := s.ls.Load(dgst)
	if !ok {
		return content.Info{}, errdefs.ErrNotFound
	}
	return s.info(dgst, fi, labels), nil
}

// isKnownAlgorithm checks is a string is a supported hash algorithm
func isKnownAlgorithm(alg string) bool {
	switch digest.Algorithm(alg) {
	case digest.SHA256, digest.SHA512, digest.SHA384:
		return true
	default:
		return false
	}
}

func (s *RevampStore) Delete(ctx context.Context, dgst digest.Digest) error {
	s.ls.Delete(dgst)
	err := s.GC(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (s *RevampStore) GC(ctx context.Context) error {
	rootpath := filepath.Join(s.root, "blobs")
	algDirs, err := os.ReadDir(rootpath)
	if err != nil {
		return err
	}
	for _, algDir := range algDirs {
		alg := algDir.Name()
		if !algDir.IsDir() || !isKnownAlgorithm(alg) {
			continue
		}
		algPath := filepath.Join(rootpath, alg)
		digestEntries, err := os.ReadDir(algPath)
		if err != nil {
			return err
		}

		for _, digestEntry := range digestEntries {
			if err := isContextDone(ctx); err != nil {
				return err
			}
			dgst := digestEntry.Name()
			blobDigest := digest.NewDigestFromEncoded(digest.Algorithm(alg), dgst)
			if err := blobDigest.Validate(); err != nil {
				// skip irrelevant content
				continue
			}
			_, ok := s.ls.Load(blobDigest)
			if !ok {
				// remove the blob from storage if it does not exist in Store
				err = os.Remove(path.Join(algPath, dgst))
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func isContextDone(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func (s *RevampStore) info(dgst digest.Digest, fi os.FileInfo, labels map[string]string) content.Info {
	return content.Info{
		Digest:    dgst,
		Size:      fi.Size(),
		CreatedAt: fi.ModTime(),
		UpdatedAt: getATime(fi),
		Labels:    labels,
	}
}

func getATime(fi os.FileInfo) time.Time {
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return time.Unix(st.Atim.Unix())
	}

	return fi.ModTime()
}

func (s *RevampStore) update(ctx context.Context, info content.Info, fieldpaths ...string) (content.Info, error) {
	p, err := s.blobPath(info.Digest)
	if err != nil {
		return content.Info{}, fmt.Errorf("calculating blob path for update: %w", err)
	}

	fi, err := os.Stat(p)
	if err != nil {
		if os.IsNotExist(err) {
			err = fmt.Errorf("content %v: %w", info.Digest, errdefs.ErrNotFound)
		}

		return content.Info{}, err
	}

	var labels map[string]string
	if len(fieldpaths) > 0 {
		for _, path := range fieldpaths {
			if strings.HasPrefix(path, "labels.") {
				if labels == nil {
					labels = map[string]string{}
				}

				key := strings.TrimPrefix(path, "labels.")
				labels[key] = info.Labels[key]
				continue
			}

			switch path {
			case "labels":
				labels = info.Labels
			default:
				return content.Info{}, fmt.Errorf("cannot update %q field on content info %q: %w", path, info.Digest, errdefs.ErrInvalidArgument)
			}
		}
	} else {
		labels = info.Labels
	}

	labels, err = s.ls.Update(info.Digest, labels)
	if err != nil {
		return content.Info{}, err
	}

	info = s.info(info.Digest, fi, labels)
	info.UpdatedAt = time.Now()

	if err := os.Chtimes(p, info.UpdatedAt, info.CreatedAt); err != nil {
		log.G(ctx).WithError(err).Warnf("could not change access time for %s", info.Digest)
	}

	return info, nil
}

func (s *RevampStore) blobPath(dgst digest.Digest) (string, error) {
	if err := dgst.Validate(); err != nil {
		return "", fmt.Errorf("cannot calculate blob path from invalid digest: %v: %w", err, errdefs.ErrInvalidArgument)
	}

	return filepath.Join(s.root, "blobs", dgst.Algorithm().String(), dgst.Encoded()), nil
}
