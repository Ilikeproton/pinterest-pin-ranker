//go:build cgo && windows

package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	comdlg32DLL                    = windows.NewLazySystemDLL("comdlg32.dll")
	comdlg32ProcGetSaveFileName    = comdlg32DLL.NewProc("GetSaveFileNameW")
	comdlg32ProcCommDlgExtendedErr = comdlg32DLL.NewProc("CommDlgExtendedError")
)

const (
	ofnOverwritePrompt = 0x00000002
	ofnNoChangeDir     = 0x00000008
	ofnPathMustExist   = 0x00000800
	ofnExplorer        = 0x00080000
)

type openFileName struct {
	lStructSize       uint32
	hwndOwner         windows.Handle
	hInstance         windows.Handle
	lpstrFilter       *uint16
	lpstrCustomFilter *uint16
	nMaxCustFilter    uint32
	nFilterIndex      uint32
	lpstrFile         *uint16
	nMaxFile          uint32
	lpstrFileTitle    *uint16
	nMaxFileTitle     uint32
	lpstrInitialDir   *uint16
	lpstrTitle        *uint16
	flags             uint32
	nFileOffset       uint16
	nFileExtension    uint16
	lpstrDefExt       *uint16
	lCustData         uintptr
	lpfnHook          uintptr
	lpTemplateName    *uint16
	pvReserved        unsafe.Pointer
	dwReserved        uint32
	flagsEx           uint32
}

func saveImageFromDesktop(owner uintptr, sourcePath, sourceURL, suggestedName string) (savedPath string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			savedPath = ""
			err = fmt.Errorf("save image: %v", recovered)
		}
	}()

	targetPath, err := chooseSaveImagePath(windows.Handle(owner), sourcePath, sourceURL, suggestedName)
	if err != nil || targetPath == "" {
		return "", err
	}

	if err := writeSavedImage(targetPath, sourcePath, sourceURL); err != nil {
		return "", err
	}
	return targetPath, nil
}

func chooseSaveImagePath(owner windows.Handle, sourcePath, sourceURL, suggestedName string) (string, error) {
	fileName := buildSaveFileName(sourcePath, sourceURL, suggestedName)
	fileBuf := make([]uint16, 1024)
	copy(fileBuf, windows.StringToUTF16(fileName))

	filter := utf16DoubleNullTerminated(
		"Image Files",
		"*.png;*.jpg;*.jpeg;*.webp;*.gif;*.bmp",
		"All Files",
		"*.*",
	)
	title := windows.StringToUTF16Ptr("Save Image")
	defExt := windows.StringToUTF16Ptr(strings.TrimPrefix(filepath.Ext(fileName), "."))

	ofn := openFileName{
		lStructSize: uint32(unsafe.Sizeof(openFileName{})),
		hwndOwner:   owner,
		lpstrFilter: &filter[0],
		lpstrFile:   &fileBuf[0],
		nMaxFile:    uint32(len(fileBuf)),
		lpstrTitle:  title,
		flags:       ofnExplorer | ofnOverwritePrompt | ofnPathMustExist | ofnNoChangeDir,
		lpstrDefExt: defExt,
	}

	ok, _, callErr := comdlg32ProcGetSaveFileName.Call(uintptr(unsafe.Pointer(&ofn)))
	if ok == 0 {
		code, _, _ := comdlg32ProcCommDlgExtendedErr.Call()
		if code == 0 {
			return "", nil
		}
		if callErr != nil && callErr != windows.ERROR_SUCCESS {
			return "", fmt.Errorf("open save dialog: %w", callErr)
		}
		return "", fmt.Errorf("open save dialog: code %d", code)
	}

	return windows.UTF16ToString(fileBuf), nil
}

func buildSaveFileName(sourcePath, sourceURL, suggestedName string) string {
	base := sanitizeSaveName(strings.TrimSpace(suggestedName))
	if base == "" {
		base = "pinpulse-image"
	}

	ext := inferSaveExt(sourcePath, sourceURL)
	if strings.HasSuffix(strings.ToLower(base), strings.ToLower(ext)) {
		return base
	}
	return base + ext
}

func utf16DoubleNullTerminated(parts ...string) []uint16 {
	joined := strings.Join(parts, "\x00") + "\x00\x00"
	return windows.StringToUTF16(joined)
}

func sanitizeSaveName(value string) string {
	replacer := strings.NewReplacer(
		"<", "_",
		">", "_",
		":", "_",
		"\"", "_",
		"/", "_",
		"\\", "_",
		"|", "_",
		"?", "_",
		"*", "_",
	)
	value = replacer.Replace(strings.TrimSpace(value))
	value = strings.Join(strings.Fields(value), " ")
	return value
}

func inferSaveExt(sourcePath, sourceURL string) string {
	if ext := strings.ToLower(filepath.Ext(strings.TrimSpace(sourcePath))); ext != "" && len(ext) <= 6 {
		return ext
	}
	if raw := strings.TrimSpace(sourceURL); raw != "" {
		if parsed, err := url.Parse(raw); err == nil {
			if ext := strings.ToLower(filepath.Ext(parsed.Path)); ext != "" && len(ext) <= 6 {
				return ext
			}
		}
	}
	return ".jpg"
}

func writeSavedImage(targetPath, sourcePath, sourceURL string) error {
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath != "" {
		return copyFileContents(targetPath, sourcePath)
	}
	return downloadToFile(targetPath, sourceURL)
}

func copyFileContents(targetPath, sourcePath string) error {
	targetAbs, targetErr := filepath.Abs(targetPath)
	sourceAbs, sourceErr := filepath.Abs(sourcePath)
	if targetErr == nil && sourceErr == nil && strings.EqualFold(targetAbs, sourceAbs) {
		return nil
	}

	src, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open source image: %w", err)
	}
	defer src.Close()

	dst, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("create target image: %w", err)
	}
	defer func() {
		_ = dst.Close()
	}()

	if _, err := io.Copy(dst, src); err != nil {
		return fmt.Errorf("copy image data: %w", err)
	}
	return dst.Close()
}

func downloadToFile(targetPath, sourceURL string) error {
	req, err := http.NewRequest(http.MethodGet, strings.TrimSpace(sourceURL), nil)
	if err != nil {
		return fmt.Errorf("prepare image download: %w", err)
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
	req.Header.Set("Referer", "https://www.pinterest.com/")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("download image: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("download image: status %d", resp.StatusCode)
	}

	dst, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("create target image: %w", err)
	}
	defer func() {
		_ = dst.Close()
	}()

	if _, err := io.Copy(dst, resp.Body); err != nil {
		return fmt.Errorf("write target image: %w", err)
	}
	return dst.Close()
}
