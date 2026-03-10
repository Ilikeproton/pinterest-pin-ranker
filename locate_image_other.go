//go:build cgo && !windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

func locateImageFromDesktop(_ uintptr, sourcePath, sourceURL string) (string, error) {
	localPath := strings.TrimSpace(sourcePath)
	if localPath != "" {
		if absPath, absErr := filepath.Abs(localPath); absErr == nil {
			localPath = absPath
		}
		if info, statErr := os.Stat(localPath); statErr == nil && !info.IsDir() {
			if err := openExternalURL(localPath); err != nil {
				return "", fmt.Errorf("open local image: %w", err)
			}
			return localPath, nil
		}
	}

	remoteURL := strings.TrimSpace(sourceURL)
	if remoteURL != "" {
		if err := openExternalURL(remoteURL); err != nil {
			return "", fmt.Errorf("open image source: %w", err)
		}
		return remoteURL, nil
	}

	return "", fmt.Errorf("no local image or source url available")
}
