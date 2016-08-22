package acona

import "path/filepath"

func SecureJoin(args ...string) string {
	if len(args) > 1 {
		s := []string{"/"}
		s = append(s, args[1:]...)
		jailedPath := filepath.Join(s...)
		return filepath.Join(args[0], jailedPath)
	}
	return filepath.Join(args...)
}
