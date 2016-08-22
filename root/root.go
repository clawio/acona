package root

import (
	"github.com/clawio/acona"
	"io"
	"strings"
	"github.com/pkg/errors"
)

type Object struct {
	prefix  string
	wrapped acona.Object
}

func (o *Object) Checksum() string      { return o.wrapped.Checksum() }
func (o *Object) ID() string            { return o.wrapped.ID() }
func (o *Object) IsDir() bool           { return o.wrapped.IsDir() }
func (o *Object) ModTime() int64        { return o.wrapped.ModTime() }
func (o *Object) MimeType() string      { return o.wrapped.MimeType() }
func (o *Object) Path() string          { return acona.SecureJoin(o.prefix, o.wrapped.Path()) }
func (o *Object) Size() int64           { return o.wrapped.Size() }
func (o *Object) Optional() interface{} { return o.wrapped.Optional() }

func newObject(prefix string, object acona.Object) acona.Object {
	o := &Object{}
	o.prefix = prefix
	o.wrapped = object
	return o
}

type Store struct {
	name, root string
	stores     []acona.Store
}

func (s *Store) getStoreFromPath(path string) (acona.Store, string, error) {
	tokens := strings.Split(strings.Trim(strings.TrimSpace(path), "/"), "/")
	if len(tokens) == 0 {
		return nil, "", acona.ErrorObjectNotFound
	}
	var pathWithoutStoreName string
	if len(tokens) > 1 {
		pathWithoutStoreName = tokens[1]
	}
	for _, store := range s.stores {
		if store.Name() == tokens[0] {
			return store, pathWithoutStoreName, nil
		}
	}
	return nil, "", acona.ErrorObjectNotFound
}

func NewStore(name, root string, stores []acona.Store) (acona.Store, error) {
	return &Store{name, root, stores}, nil
}
func (s *Store) Name() string { return s.name }
func (s *Store) Root() string { return s.root }

func (s *Store) PutObject(reader io.Reader, path, inputHash string) error {
	store, path, err := s.getStoreFromPath(path)
	if err != nil {
		return err
	}
	return store.PutObject(reader, path, inputHash)
}

func (s *Store) GetObject(path string) (io.ReadCloser, error) {
	store, path, err := s.getStoreFromPath(path)
	if err != nil {
		return nil, err
	}
	return store.GetObject(path)
}

func (s *Store) Examine(path string) (acona.Object, error) {
	store, path, err := s.getStoreFromPath(path)
	if err != nil {
		return nil, err
	}
	object, err := store.Examine(path)
	if err != nil {
		return nil, errors.Wrap(err, "can't examine")
	}
	return newObject(store.Name(), object), nil
}

func (s *Store) listRoot() ([]acona.Object, error) {
	var objects []acona.Object
	for _, store := range s.stores {
		object, err := store.Examine("")
		if err != nil {
			return nil, errors.Wrap(err, "can't examine")
		}
		objects = append(objects, newObject(store.Name(), object))
	}
	return objects, nil
}

func (s *Store) ListTree(path string) ([]acona.Object, error) {
	if path == "" {
		objects, err := s.listRoot()
		if err != nil {
			return nil, err
		}
		return objects, nil
	}
	store, path, err := s.getStoreFromPath(path)
	if err != nil {
		return nil, err
	}
	objects, err := store.ListTree(path)
	if err != nil {
		return nil, errors.Wrapf(err, "can't list tree - %s", path)
	}
	objectsPrefixed := []acona.Object{}
	for _, o := range objects {
		objectsPrefixed = append(objectsPrefixed, newObject(store.Name(), o))
	}
	return objectsPrefixed, nil
}

func (s *Store) Remove(path string) error {
	store, path, err := s.getStoreFromPath(path)
	if err != nil {
		return err
	}
	return store.Remove(path)
}

func (s *Store) Rename(srcPath, destPath string) error {
	srcStore, srcPath, err := s.getStoreFromPath(srcPath)
	if err != nil {
		return err
	}
	destStore, destPath, err := s.getStoreFromPath(destPath)
	if err != nil {
		return err
	}
	if srcStore.Name() != destStore.Name() {
		return acona.ErrorCantMove
	}
	return srcStore.Rename(srcPath, destPath)
}
