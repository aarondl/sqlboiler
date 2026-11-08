package boilingcore

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestFindGoModPathStopsAtFilesystemRoot(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("the regression is specific to Windows drive roots")
	}

	root := filepath.VolumeName(os.TempDir()) + string(filepath.Separator)
	calls := 0
	path, err := findGoModPath(root, func(string) (os.FileInfo, error) {
		calls++
		if calls > 1 {
			return nil, errors.New("filesystem root checked repeatedly")
		}
		return nil, os.ErrNotExist
	})

	if calls != 1 {
		t.Fatalf("stat called %d times at filesystem root; want 1", calls)
	}
	if path != "" {
		t.Fatalf("findGoModPath() path = %q; want no path", path)
	}
	if err == nil || err.Error() != "could not find go.mod in any parent directory" {
		t.Fatalf("findGoModPath() error = %v; want missing go.mod error", err)
	}
}
