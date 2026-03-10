//go:build cgo && windows

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func locateImageFromDesktop(owner uintptr, sourcePath, sourceURL string) (located string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			located = ""
			err = fmt.Errorf("locate image: %v", recovered)
		}
	}()

	localPath := strings.TrimSpace(sourcePath)
	if localPath != "" {
		if absPath, absErr := filepath.Abs(localPath); absErr == nil {
			localPath = absPath
		}
		if info, statErr := os.Stat(localPath); statErr == nil && !info.IsDir() {
			cmd := exec.Command("explorer", "/select,", localPath)
			if err := cmd.Start(); err != nil {
				return "", fmt.Errorf("locate local image: %w", err)
			}
			minimizeDesktopWindowHandle(owner)
			return localPath, nil
		}
	}

	remoteURL := strings.TrimSpace(sourceURL)
	if remoteURL != "" {
		if err := openExternalURL(remoteURL); err != nil {
			return "", fmt.Errorf("open image source: %w", err)
		}
		minimizeDesktopWindowHandle(owner)
		return remoteURL, nil
	}

	return "", fmt.Errorf("no local image or source url available")
}
