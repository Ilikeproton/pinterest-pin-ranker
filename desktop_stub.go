//go:build !cgo || !(windows || darwin || linux)

package main

import "log"

func runDesktopShell(app *pinPulseApp, _ bool) error {
	srv, ln, uiURL, err := app.prepareServer("127.0.0.1:0")
	if err != nil {
		return err
	}

	settings, settingsErr := app.store.GetSettings()
	if settingsErr == nil {
		log.Printf("Concurrency=%d, GlobalRunning=%v", settings.Concurrency, settings.GlobalRunning)
	}
	log.Printf("desktop shell unavailable in this build; falling back to browser mode")
	log.Printf("PinPulse data dir: %s", app.dataDir)
	log.Printf("PinPulse server started on %s", uiURL)

	if err := openExternalURL(uiURL); err != nil {
		log.Printf("open browser warning: %v", err)
	}

	return srv.Serve(ln)
}
