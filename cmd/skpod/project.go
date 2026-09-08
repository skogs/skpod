package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

const maxGitPathFileBytes = 4096

func currentProjectRoot() (string, error) {
	workingDirectory, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("read current working directory: %w", err)
	}
	return projectRoot(workingDirectory)
}

func projectRoot(start string) (string, error) {
	absolute, err := filepath.Abs(start)
	if err != nil {
		return "", fmt.Errorf("resolve absolute project path: %w", err)
	}
	absolute, err = filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("resolve canonical project path: %w", err)
	}

	for candidate := filepath.Clean(absolute); ; candidate = filepath.Dir(candidate) {
		marker := filepath.Join(candidate, ".git")
		info, markerErr := os.Stat(marker)
		if markerErr == nil {
			if info.IsDir() {
				return normalizeProjectPath(candidate), nil
			}
			if !info.Mode().IsRegular() {
				return "", fmt.Errorf("inspect project marker %s: expected a directory or regular gitfile", marker)
			}
			common, err := gitFileProjectRoot(candidate, marker)
			if err != nil {
				return "", err
			}
			return normalizeProjectPath(common), nil
		}
		if !errors.Is(markerErr, os.ErrNotExist) {
			return "", fmt.Errorf("inspect project marker in %s: %w", candidate, markerErr)
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return normalizeProjectPath(absolute), nil
		}
	}
}

// gitFileProjectRoot resolves the documented gitfile and commondir indirection
// used by linked worktrees. It deliberately reads Git's on-disk metadata rather
// than requiring the git executable at runtime.
func gitFileProjectRoot(worktreeRoot, marker string) (string, error) {
	gitDir, err := readGitPathFile(marker, "gitdir:")
	if err != nil {
		return "", fmt.Errorf("inspect worktree marker %s: %w", marker, err)
	}
	gitDir = resolveMetadataPath(worktreeRoot, gitDir)
	gitDir, err = filepath.EvalSymlinks(gitDir)
	if err != nil {
		return "", fmt.Errorf("resolve worktree git directory %s: %w", gitDir, err)
	}

	commonDir := gitDir
	commonFile := filepath.Join(gitDir, "commondir")
	commonPath, err := readGitPathFile(commonFile, "")
	if err == nil {
		commonDir = resolveMetadataPath(gitDir, commonPath)
		commonDir, err = filepath.EvalSymlinks(commonDir)
		if err != nil {
			return "", fmt.Errorf("resolve common git directory %s: %w", commonDir, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect common git directory metadata %s: %w", commonFile, err)
	}

	// A normal repository's common directory is <primary-worktree>/.git.
	// Returning its parent preserves the existing project identity while making
	// every linked worktree resolve to that same identity.
	if filepath.Base(commonDir) == ".git" {
		return filepath.Dir(commonDir), nil
	}
	// Separate-git-dir layouts have no working-tree path encoded in commondir;
	// the common directory itself is their stable shared identity.
	return commonDir, nil
}

func readGitPathFile(path, prefix string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()

	content, err := io.ReadAll(io.LimitReader(file, maxGitPathFileBytes+1))
	if err != nil {
		return "", err
	}
	if len(content) > maxGitPathFileBytes {
		return "", fmt.Errorf("metadata exceeds %d bytes", maxGitPathFileBytes)
	}
	value := strings.TrimSpace(string(content))
	if strings.ContainsAny(value, "\r\n\x00") {
		return "", errors.New("metadata must contain exactly one path")
	}
	if prefix != "" {
		if !strings.HasPrefix(value, prefix) {
			return "", fmt.Errorf("metadata must begin with %q", prefix)
		}
		value = strings.TrimSpace(strings.TrimPrefix(value, prefix))
	}
	if value == "" {
		return "", errors.New("metadata path is empty")
	}
	return value, nil
}

func resolveMetadataPath(base, value string) string {
	if filepath.IsAbs(value) {
		return filepath.Clean(value)
	}
	return filepath.Clean(filepath.Join(base, value))
}

func normalizeProjectPath(value string) string {
	value = filepath.Clean(value)
	if runtime.GOOS == "windows" {
		value = strings.ToLower(value)
	}
	return value
}
