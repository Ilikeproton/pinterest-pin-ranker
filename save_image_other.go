//go:build cgo && !windows

package main

import "fmt"

func saveImageFromDesktop(_ uintptr, _, _, _ string) (string, error) {
	return "", fmt.Errorf("native save dialog is only implemented on Windows in this build")
}
