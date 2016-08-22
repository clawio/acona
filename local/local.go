package local

import (
	"github.com/clawio/acona"
	"github.com/pkg/errors"
	"io"
	"io/ioutil"
	"mime"
	"os"
	"path"
	"path/filepath"
)

type Config struct {
	RootDir, TempDir string
	CheckInputHash   bool
}

type Object struct {
	fileInfo os.FileInfo
	path     string
}

func (o *Object) Checksum() string      { return "" }
func (o *Object) ID() string            { return o.path }
func (o *Object) IsDir() bool           { return o.fileInfo.IsDir() }
func (o *Object) ModTime() int64        { return o.fileInfo.ModTime().Unix() }
func (o *Object) MimeType() string      { return mime.TypeByExtension(path.Ext(o.Path())) }
func (o *Object) Path() string          { return o.path }
func (o *Object) Size() int64           { return o.fileInfo.Size() }
func (o *Object) Optional() interface{} { return nil }

type Store struct {
	name           string
	root           string
	rootDir        string
	tempDir        string
	checkInputHash bool
}

func NewStore(name, root string, config Config) (acona.Store, error) {
	store := &Store{}
	store.name = name
	store.root = root
	store.rootDir = acona.SecureJoin(config.RootDir, root)
	store.tempDir = config.TempDir

	// check for defaults
	if store.tempDir == "" {
		store.tempDir = os.TempDir()
	}

	return store, nil
}

func (s *Store) Name() string { return s.name }
func (s *Store) Root() string { return s.root }

func (s *Store) PutObject(reader io.Reader, path, inputHash string) error {
	tmp, err := s.saveToTempFile(reader)
	if err != nil {
		return errors.Wrap(err, "can't save to temp file")
	}

	if s.checkInputHash {
		inputHashType := acona.HashTypeFromString(inputHash)
		if inputHashType != acona.HashNone {
			computedHash, err := acona.HashStream(reader)
			if err != nil {
				return errors.Wrap(err, "can't compute hash")
			}

			inputHashValue := acona.HashValueFromString(inputHash)
			if computedHash[inputHashType] != inputHashValue {
				return acona.ErrHashesNotMatch
			}
		}
	}

	localPath := s.getLocalPath(path)
	if err := os.Rename(tmp, localPath); err != nil {
		if os.IsNotExist(err) {
			return acona.ErrorObjectNotFound
		}
		return errors.Wrap(err, "can't rename tmp file to target")
	}

	return nil
}

func (s *Store) GetObject(path string) (io.ReadCloser, error) {
	localPath := s.getLocalPath(path)
	fd, err := os.Open(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, acona.ErrorObjectNotFound
		}
		return nil, errors.Wrap(err, "can't open local file")
	}
	return fd, nil
}

func (s *Store) Examine(path string) (acona.Object, error) {
	localPath := s.getLocalPath(path)
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, acona.ErrorObjectNotFound
		}
		return nil, errors.Wrap(err, "can't examine")
	}
	object := newObject(fileInfo, path)
	return object, nil
}

func (s *Store) ListTree(path string) ([]acona.Object, error) {
	localPath := s.getLocalPath(path)
	fileInfo, err := os.Stat(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, acona.ErrorObjectNotFound
		}
		return nil, errors.Wrap(err, "can't stat")
	}
	if !fileInfo.IsDir() {
		return nil, acona.ErrorIsFile
	}
	fd, err := os.Open(localPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, acona.ErrorObjectNotFound
		}
		return nil, err
	}
	fileInfos, err := fd.Readdir(-1) // read all files inside the directory.
	if err != nil {
		return nil, errors.Wrap(err, "can't read dir")
	}

	var objects []acona.Object
	for _, fi := range fileInfos {
		childPath := filepath.Join(path, filepath.Base(fi.Name()))
		objects = append(objects, newObject(fi, childPath))
	}

	return objects, nil
}

func (s *Store) Remove(path string) error {
	localPath := s.getLocalPath(path)
	if err := os.RemoveAll(localPath); err != nil {
		return errors.Wrap(err, "can't remove")
	}
	return nil
}

func (s *Store) Rename(srcPath, destPath string) error {
	srcLocalPath := s.getLocalPath(srcPath)
	destLocalPath := s.getLocalPath(destPath)
	if err := os.Rename(srcLocalPath, destLocalPath); err != nil {
		if os.IsNotExist(err) {
			return acona.ErrorObjectNotFound
		} else if _, ok := err.(*os.LinkError); ok {
			return acona.ErrorCantMove
		}
		return errors.Wrap(err, "can't move")
	}
	return nil
}

func (s *Store) getLocalPath(path string) string {
	return acona.SecureJoin(s.rootDir, path)
}

func (s *Store) saveToTempFile(r io.Reader) (string, error) {
	fd, err := ioutil.TempFile(s.tempDir, "")
	defer fd.Close()
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(fd, r); err != nil {
		return "", err
	}
	return fd.Name(), nil
}

func newObject(fileInfo os.FileInfo, path string) acona.Object {
	return &Object{fileInfo: fileInfo, path: path}
}
