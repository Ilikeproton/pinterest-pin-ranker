//go:build cgo && (windows || darwin || linux)

package main

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"time"
)

func runDesktopShell(app *pinPulseApp, debug bool) error {
	srv, ln, uiURL, err := app.prepareServer("127.0.0.1:0")
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(ln)
	}()

	w, err := newDesktopWebView(debug)
	if err != nil {
		_ = ln.Close()
		return err
	}
	if w == nil {
		_ = ln.Close()
		return errors.New("create webview failed")
	}
	defer destroyDesktopWindow(w)
	defer w.Destroy()

	w.SetTitle("PinPulse")
	applyDesktopWindowFrame(w)

	if err := w.Bind("pinpulseOpenExternal", func(rawURL string) error {
		return openExternalURL(rawURL)
	}); err != nil {
		return err
	}
	if err := w.Bind("pinpulseWindowMinimize", func() error {
		return minimizeDesktopWindow(w)
	}); err != nil {
		return err
	}
	if err := w.Bind("pinpulseWindowClose", func() error {
		return closeDesktopWindow(w)
	}); err != nil {
		return err
	}
	if err := w.Bind("pinpulseLocateImage", func(sourcePath, sourceURL string) (string, error) {
		return locateImageFromDesktop(desktopWindowHandle(w), sourcePath, sourceURL)
	}); err != nil {
		return err
	}
	if err := w.Bind("pinpulseDesktopReady", func() error {
		return showDesktopWindow(w)
	}); err != nil {
		return err
	}

	w.Navigate(startupLoadingURL(uiURL))
	w.Run()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)

	serveErr := <-errCh
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return serveErr
	}
	return nil
}

func startupLoadingURL(uiURL string) string {
	values := url.Values{}
	values.Set("target", uiURL)
	return uiURL + "/static/startup.html?" + values.Encode()
}
