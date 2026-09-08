package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProjectRootUsesNearestGitMarker(t *testing.T) {
	for _, marker := range []string{"directory", "file"} {
		t.Run(marker, func(t *testing.T) {
			root := t.TempDir()
			gitMarker := filepath.Join(root, ".git")
			var err error
			if marker == "directory" {
				err = os.Mkdir(gitMarker, 0o755)
			} else {
				err = os.WriteFile(gitMarker, []byte("gitdir: elsewhere"), 0o600)
			}
			if err != nil {
				t.Fatal(err)
			}
			nested := filepath.Join(root, "apps", "api")
			if err := os.MkdirAll(nested, 0o755); err != nil {
				t.Fatal(err)
			}

			got, err := projectRoot(nested)
			if err != nil {
				t.Fatal(err)
			}
			want, err := filepath.EvalSymlinks(root)
			if err != nil {
				t.Fatal(err)
			}
			want = normalizeProjectPath(want)
			if got != want {
				t.Fatalf("projectRoot(%q) = %q, want %q", nested, got, want)
			}
		})
	}
}

func TestProjectRootFallsBackToStartingDirectory(t *testing.T) {
	start := filepath.Join(t.TempDir(), "standalone")
	if err := os.Mkdir(start, 0o755); err != nil {
		t.Fatal(err)
	}

	got, err := projectRoot(start)
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(start)
	if err != nil {
		t.Fatal(err)
	}
	want = normalizeProjectPath(want)
	if got != want {
		t.Fatalf("projectRoot(%q) = %q, want %q", start, got, want)
	}
}
