package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestProjectRootUsesNearestGitMarker(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
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
}

func TestProjectRootSharesIdentityAcrossLinkedWorktree(t *testing.T) {
	base := t.TempDir()
	primary := filepath.Join(base, "primary")
	common := filepath.Join(primary, ".git")
	admin := filepath.Join(common, "worktrees", "coder")
	linked := filepath.Join(base, "coder")
	if err := os.MkdirAll(admin, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(linked, "internal", "pkg"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(linked, ".git"), []byte("gitdir: "+admin+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(admin, "commondir"), []byte("../..\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	primaryProject, err := projectRoot(primary)
	if err != nil {
		t.Fatal(err)
	}
	linkedProject, err := projectRoot(filepath.Join(linked, "internal", "pkg"))
	if err != nil {
		t.Fatal(err)
	}
	if linkedProject != primaryProject {
		t.Fatalf("linked project = %q, primary project = %q", linkedProject, primaryProject)
	}
}

func TestProjectRootRejectsMalformedWorktreeMetadata(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".git"), []byte("not a gitdir"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := projectRoot(root); err == nil {
		t.Fatal("projectRoot accepted malformed .git file")
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
