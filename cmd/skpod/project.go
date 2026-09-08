package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

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
		_, markerErr := os.Stat(filepath.Join(candidate, ".git"))
		if markerErr == nil {
			return normalizeProjectPath(candidate), nil
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

func normalizeProjectPath(value string) string {
	value = filepath.Clean(value)
	if runtime.GOOS == "windows" {
		value = strings.ToLower(value)
	}
	return value
}
