package main

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

type pinPulseApp struct {
	dataDir   string
	imageDir  string
	dbPath    string
	db        *sql.DB
	store     *Store
	scheduler *Scheduler
}

func newPinPulseApp(dataDirOverride string) (*pinPulseApp, error) {
	dataDir, err := resolveDataDir(dataDirOverride)
	if err != nil {
		return nil, err
	}

	imageDir := filepath.Join(dataDir, "images")
	if err := os.MkdirAll(imageDir, 0o755); err != nil {
		return nil, fmt.Errorf("create image dir: %w", err)
	}

	dbPath := filepath.Join(dataDir, "app.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	store, err := NewStore(db, imageDir)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("init store: %w", err)
	}

	compactCtx, compactCancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer compactCancel()
	compacted, compactErr := store.CompactEligibleBatches(compactCtx)
	if compactErr != nil {
		log.Printf("startup compact warning: %v", compactErr)
	} else if compacted > 0 {
		log.Printf("Compacted %d full batch(es) on startup", compacted)
	}

	settings, err := store.GetSettings()
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load settings: %w", err)
	}

	crawler := NewCrawler(imageDir)
	if err := crawler.UpdateProxyConfig(settings.ProxyConfig()); err != nil {
		log.Printf("proxy settings warning: %v", err)
	}
	scheduler := NewScheduler(store, crawler, settings.Concurrency, settings.GlobalRunning)
	scheduler.Start()

	return &pinPulseApp{
		dataDir:   dataDir,
		imageDir:  imageDir,
		dbPath:    dbPath,
		db:        db,
		store:     store,
		scheduler: scheduler,
	}, nil
}

func (a *pinPulseApp) Close() error {
	if a.scheduler != nil {
		a.scheduler.Stop()
	}
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

func (a *pinPulseApp) RunServer(addr string) error {
	srv, ln, uiURL, err := a.prepareServer(addr)
	if err != nil {
		return err
	}

	settings, err := a.store.GetSettings()
	if err == nil {
		log.Printf("Concurrency=%d, GlobalRunning=%v", settings.Concurrency, settings.GlobalRunning)
	}
	log.Printf("PinPulse data dir: %s", a.dataDir)
	log.Printf("PinPulse server started on %s", uiURL)

	return srv.Serve(ln)
}

func (a *pinPulseApp) RunDesktop(debug bool) error {
	return runDesktopShell(a, debug)
}

func (a *pinPulseApp) prepareServer(addr string) (*http.Server, net.Listener, string, error) {
	ln, err := net.Listen("tcp", a.serverAddress(addr))
	if err != nil {
		return nil, nil, "", fmt.Errorf("listen ui: %w", err)
	}

	srv := &http.Server{
		Addr:              ln.Addr().String(),
		Handler:           a.newHandler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       20 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	return srv, ln, "http://" + ln.Addr().String(), nil
}

func (a *pinPulseApp) newHandler() http.Handler {
	return NewAPI(a.store, a.scheduler, a.imageDir, embeddedWebSubFS).Routes()
}

func (a *pinPulseApp) serverAddress(raw string) string {
	addr := strings.TrimSpace(raw)
	if addr == "" {
		return "127.0.0.1:0"
	}
	if strings.HasPrefix(addr, ":") {
		return "127.0.0.1" + addr
	}
	return addr
}

func resolveDataDir(override string) (string, error) {
	if value := strings.TrimSpace(override); value != "" {
		return filepath.Abs(value)
	}
	if value := strings.TrimSpace(os.Getenv("PINPULSE_DATA_DIR")); value != "" {
		return filepath.Abs(value)
	}

	if wd, err := os.Getwd(); err == nil {
		if exists(filepath.Join(wd, "go.mod")) {
			return filepath.Abs(filepath.Join(wd, "data"))
		}
	}

	exePath, err := os.Executable()
	if err == nil {
		exeDir := filepath.Dir(exePath)
		portableDataDir := filepath.Join(exeDir, "data")
		if runtime.GOOS == "windows" || exists(portableDataDir) {
			return filepath.Abs(portableDataDir)
		}
	}

	configDir, err := os.UserConfigDir()
	if err == nil {
		return filepath.Abs(filepath.Join(configDir, "PinPulse"))
	}

	return filepath.Abs("data")
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func mustSubFS(root fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(root, dir)
	if err != nil {
		panic(err)
	}
	return sub
}

func clampConcurrency(v int) int {
	if v < 1 {
		return 1
	}
	if v > 32 {
		return 32
	}
	return v
}
