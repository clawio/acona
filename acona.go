package acona

import (
	"io"
	"github.com/pkg/errors"
)

var (
	ErrorCantPurge            = errors.New("can't purge directory")
	ErrorCantCopy             = errors.New("can't copy object - incompatible remotes")
	ErrorCantMove             = errors.New("can't move object - incompatible remotes")
	ErrorCantDirMove          = errors.New("can't move directory - incompatible remotes")
	ErrorDirExists            = errors.New("can't copy directory - destination already exists")
	ErrorCantSetModTime       = errors.New("can't set modified time")
	ErrorDirNotFound          = errors.New("directory not found")
	ErrorObjectNotFound       = errors.New("object not found")
	ErrorLevelNotSupported    = errors.New("level value not supported")
	ErrorListAborted          = errors.New("list aborted")
	ErrorListOnlyRoot         = errors.New("can only list from root")
	ErrorIsFile               = errors.New("is a file not a directory")
	ErrorNotDeleting          = errors.New("not deleting files as there were IO errors")
	ErrorCantMoveOverlapping  = errors.New("can't move files on overlapping remotes")
)

type Object interface {
	Checksum() string
	ID() string
	IsDir() bool
	ModTime() int64
	MimeType() string
	Path() string
	Size() int64
	Optional() interface{}
}

type Store interface {
	Name() string
	Root() string

	PutObject(reader io.Reader, path,  clientChecksum string) error
	GetObject(path string) (io.ReadCloser, error)

	Examine(path string) (Object, error)
	ListTree(path string) ([]Object, error)
	Remove(path string) error
	Rename(sourcePath, targetPath string) error
}
