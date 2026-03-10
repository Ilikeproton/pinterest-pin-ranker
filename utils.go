package main

import (
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	pinPathRegex = regexp.MustCompile(`/pin/(\d+)`)
	digitsRegex  = regexp.MustCompile(`\d+`)
)

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func normalizePinURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty url")
	}

	if strings.HasPrefix(raw, "//") {
		raw = "https:" + raw
	}
	if strings.HasPrefix(raw, "/pin/") {
		raw = "https://www.pinterest.com" + raw
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "https://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse url: %w", err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("invalid url host")
	}

	host := strings.ToLower(u.Host)
	if !strings.Contains(host, "pinterest.") {
		return "", fmt.Errorf("not pinterest host")
	}

	match := pinPathRegex.FindStringSubmatch(u.Path)
	if len(match) < 2 {
		return "", fmt.Errorf("not a pin page")
	}

	return fmt.Sprintf("https://www.pinterest.com/pin/%s/", match[1]), nil
}

func pinIDFromURL(raw string) string {
	match := pinPathRegex.FindStringSubmatch(raw)
	if len(match) < 2 {
		return ""
	}
	return match[1]
}

func parseCount(raw string) int {
	raw = strings.TrimSpace(strings.ToLower(raw))
	if raw == "" {
		return 0
	}
	raw = strings.ReplaceAll(raw, ",", "")
	raw = strings.ReplaceAll(raw, " ", "")

	multiplier := 1.0
	switch {
	case strings.HasSuffix(raw, "k"):
		multiplier = 1000
		raw = strings.TrimSuffix(raw, "k")
	case strings.HasSuffix(raw, "m"):
		multiplier = 1000000
		raw = strings.TrimSuffix(raw, "m")
	case strings.HasSuffix(raw, "w"):
		multiplier = 10000
		raw = strings.TrimSuffix(raw, "w")
	}

	if value, err := strconv.ParseFloat(raw, 64); err == nil {
		return int(value*multiplier + 0.5)
	}

	onlyDigits := strings.Join(digitsRegex.FindAllString(raw, -1), "")
	if onlyDigits == "" {
		return 0
	}
	value, err := strconv.Atoi(onlyDigits)
	if err != nil {
		return 0
	}
	return value
}

func chooseImageExt(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return ".jpg"
	}
	ext := strings.ToLower(path.Ext(u.Path))
	switch ext {
	case ".jpg", ".jpeg", ".png", ".webp", ".gif":
		return ext
	default:
		return ".jpg"
	}
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func intToBool(v int) bool {
	return v != 0
}

func toWebImageURL(localPath string) string {
	if strings.TrimSpace(localPath) == "" {
		return ""
	}
	normalized := strings.ReplaceAll(localPath, "\\", "/")
	return "/images/" + path.Base(normalized)
}
