//go:build cgo && !windows

package main

import webview "github.com/webview/webview_go"

func newDesktopWebView(debug bool) (webview.WebView, error) {
	return webview.New(debug), nil
}

func destroyDesktopWindow(_ webview.WebView) {}

func applyDesktopWindowFrame(w webview.WebView) {
	w.SetSize(1600, 1000, webview.HintNone)
}

func showDesktopWindow(_ webview.WebView) error {
	return nil
}

func minimizeDesktopWindow(_ webview.WebView) error {
	return nil
}

func closeDesktopWindow(w webview.WebView) error {
	w.Terminate()
	return nil
}
