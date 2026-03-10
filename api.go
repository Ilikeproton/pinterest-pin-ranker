package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type API struct {
	store     *Store
	scheduler *Scheduler
	crawler   *Crawler
	imageDir  string
	webFS     fs.FS
}

func NewAPI(store *Store, scheduler *Scheduler, imageDir string, webFS fs.FS) *API {
	var crawler *Crawler
	if scheduler != nil {
		crawler = scheduler.crawler
	}
	return &API{
		store:     store,
		scheduler: scheduler,
		crawler:   crawler,
		imageDir:  imageDir,
		webFS:     webFS,
	}
}

func (a *API) Routes() http.Handler {
	mux := http.NewServeMux()
	staticHandler := http.FileServer(http.FS(a.webFS))

	mux.HandleFunc("/api/health", a.handleHealth)
	mux.HandleFunc("/api/dashboard", a.handleDashboard)
	mux.HandleFunc("/api/settings", a.handleSettings)
	mux.HandleFunc("/api/control/start", a.handleGlobalStart)
	mux.HandleFunc("/api/control/stop", a.handleGlobalStop)
	mux.HandleFunc("/api/maintenance/archive-done", a.handleArchiveDone)
	mux.HandleFunc("/api/batches", a.handleBatches)
	mux.HandleFunc("/api/batches/", a.handleBatchRoutes)

	mux.Handle("/images/", http.StripPrefix("/images/", http.FileServer(http.Dir(a.imageDir))))
	mux.Handle("/static/", http.StripPrefix("/static/", staticHandler))

	mux.HandleFunc("/batch/", a.handleBatchPage)
	mux.HandleFunc("/", a.handleIndexPage)

	return a.logging(mux)
}

func (a *API) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (a *API) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}

	settings, err := a.store.GetSettings()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	settings.Concurrency = a.scheduler.Concurrency()
	settings.GlobalRunning = a.scheduler.GlobalRunning()

	ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer cancel()
	batches, err := a.store.ListBatches(ctx)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	statsCtx, statsCancel := context.WithTimeout(r.Context(), 12*time.Second)
	defer statsCancel()
	stats, err := a.store.RuntimeStats(statsCtx)
	if err != nil {
		log.Printf("dashboard runtime stats degraded: %v", err)
		stats = RuntimeStats{}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"settings": settings,
		"batches":  batches,
		"stats":    stats,
	})
}

func (a *API) handleSettings(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		settings, err := a.store.GetSettings()
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		settings.Concurrency = a.scheduler.Concurrency()
		settings.GlobalRunning = a.scheduler.GlobalRunning()
		writeJSON(w, http.StatusOK, settings)
		return

	case http.MethodPut:
		var payload struct {
			Concurrency           *int    `json:"concurrency"`
			DefaultBatchMaxImages *int    `json:"default_batch_max_images"`
			ProxyType             *string `json:"proxy_type"`
			ProxyHost             *string `json:"proxy_host"`
			ProxyPort             *int    `json:"proxy_port"`
			ProxyUsername         *string `json:"proxy_username"`
			ProxyPassword         *string `json:"proxy_password"`
		}
		if err := decodeJSON(r, &payload); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		hasProxyUpdate := payload.ProxyType != nil ||
			payload.ProxyHost != nil ||
			payload.ProxyPort != nil ||
			payload.ProxyUsername != nil ||
			payload.ProxyPassword != nil
		if payload.Concurrency == nil && payload.DefaultBatchMaxImages == nil && !hasProxyUpdate {
			writeError(w, http.StatusBadRequest, "at least one setting is required")
			return
		}

		if payload.Concurrency != nil {
			if *payload.Concurrency <= 0 {
				writeError(w, http.StatusBadRequest, "concurrency must be >= 1")
				return
			}
			value := clampConcurrency(*payload.Concurrency)
			if err := a.store.UpdateConcurrency(value); err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			a.scheduler.SetConcurrency(value)
		}

		if payload.DefaultBatchMaxImages != nil {
			if *payload.DefaultBatchMaxImages <= 0 {
				writeError(w, http.StatusBadRequest, "default_batch_max_images must be >= 1")
				return
			}
			if err := a.store.UpdateDefaultBatchMaxImages(*payload.DefaultBatchMaxImages); err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
		}

		if hasProxyUpdate {
			currentSettings, err := a.store.GetSettings()
			if err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}

			config := currentSettings.ProxyConfig()
			if payload.ProxyType != nil {
				config.Type = *payload.ProxyType
			}
			if payload.ProxyHost != nil {
				config.Host = *payload.ProxyHost
			}
			if payload.ProxyPort != nil {
				config.Port = *payload.ProxyPort
			}
			if payload.ProxyUsername != nil {
				config.Username = *payload.ProxyUsername
			}
			if payload.ProxyPassword != nil {
				config.Password = *payload.ProxyPassword
			}

			config, err = normalizeProxyConfig(config)
			if err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			if err := a.store.UpdateProxySettings(config); err != nil {
				writeError(w, http.StatusInternalServerError, err.Error())
				return
			}
			if a.crawler != nil {
				if err := a.crawler.UpdateProxyConfig(config); err != nil {
					writeError(w, http.StatusInternalServerError, err.Error())
					return
				}
			}
		}

		settings, _ := a.store.GetSettings()
		settings.Concurrency = a.scheduler.Concurrency()
		settings.GlobalRunning = a.scheduler.GlobalRunning()
		writeJSON(w, http.StatusOK, settings)
		return
	default:
		writeMethodNotAllowed(w)
	}
}

func (a *API) handleGlobalStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if err := a.store.SetGlobalRunning(true); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.scheduler.SetGlobalRunning(true)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "global_running": true})
}

func (a *API) handleGlobalStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if err := a.store.SetGlobalRunning(false); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	a.scheduler.SetGlobalRunning(false)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "global_running": false})
}

func (a *API) handleArchiveDone(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}

	var payload struct {
		Limit int `json:"limit"`
	}
	if r.ContentLength > 0 {
		if err := decodeJSON(r, &payload); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	archived, err := a.store.ArchiveDoneTasks(ctx, payload.Limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"archived": archived,
	})
}

func (a *API) handleBatches(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ctx, cancel := context.WithTimeout(r.Context(), 12*time.Second)
		defer cancel()
		batches, err := a.store.ListBatches(ctx)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": batches})
		return

	case http.MethodPost:
		var payload struct {
			Name      string `json:"name"`
			SeedURL   string `json:"seed_url"`
			Threshold int    `json:"threshold"`
			MaxImages int    `json:"max_images"`
			MaxDepth  int    `json:"max_depth"`
		}
		if err := decodeJSON(r, &payload); err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		payload.SeedURL = strings.TrimSpace(payload.SeedURL)
		if payload.SeedURL == "" {
			writeError(w, http.StatusBadRequest, "seed_url is required")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		batch, err := a.store.CreateBatch(ctx, payload.Name, payload.SeedURL, payload.Threshold, payload.MaxImages, payload.MaxDepth)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeJSON(w, http.StatusCreated, batch)
		return

	default:
		writeMethodNotAllowed(w)
	}
}

func (a *API) handleBatchRoutes(w http.ResponseWriter, r *http.Request) {
	tail := strings.TrimPrefix(r.URL.Path, "/api/batches/")
	tail = strings.Trim(tail, "/")
	if tail == "" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	parts := strings.Split(tail, "/")
	batchID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || batchID <= 0 {
		writeError(w, http.StatusBadRequest, "invalid batch id")
		return
	}

	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			batch, err := a.store.GetBatch(ctx, batchID)
			if err != nil {
				status := http.StatusInternalServerError
				if strings.Contains(err.Error(), "not found") {
					status = http.StatusNotFound
				}
				writeError(w, status, err.Error())
				return
			}
			writeJSON(w, http.StatusOK, batch)
			return

		case http.MethodPut:
			var payload struct {
				Threshold *int `json:"threshold"`
				MaxDepth  *int `json:"max_depth"`
			}
			if err := decodeJSON(r, &payload); err != nil {
				writeError(w, http.StatusBadRequest, err.Error())
				return
			}
			if payload.Threshold == nil && payload.MaxDepth == nil {
				writeError(w, http.StatusBadRequest, "threshold or max_depth is required")
				return
			}
			if payload.Threshold != nil && *payload.Threshold <= 0 {
				writeError(w, http.StatusBadRequest, "threshold must be >= 1")
				return
			}
			if payload.MaxDepth != nil && *payload.MaxDepth <= 0 {
				writeError(w, http.StatusBadRequest, "max_depth must be >= 1")
				return
			}

			ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
			defer cancel()
			batch, err := a.store.UpdateBatchRules(ctx, batchID, payload.Threshold, payload.MaxDepth)
			if err != nil {
				status := http.StatusInternalServerError
				if strings.Contains(err.Error(), "not found") {
					status = http.StatusNotFound
				} else if strings.Contains(err.Error(), "threshold") || strings.Contains(err.Error(), "max_depth") {
					status = http.StatusBadRequest
				}
				writeError(w, status, err.Error())
				return
			}
			a.scheduler.wake()
			writeJSON(w, http.StatusOK, batch)
			return

		case http.MethodDelete:
			ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
			defer cancel()

			deletedImages, err := a.store.DeleteBatch(ctx, batchID)
			if err != nil {
				status := http.StatusInternalServerError
				if strings.Contains(err.Error(), "not found") {
					status = http.StatusNotFound
				}
				writeError(w, status, err.Error())
				return
			}
			a.scheduler.wake()
			writeJSON(w, http.StatusOK, map[string]any{
				"ok":             true,
				"batch_id":       batchID,
				"deleted_images": deletedImages,
			})
			return

		default:
			writeMethodNotAllowed(w)
			return
		}
	}

	action := parts[1]
	switch action {
	case "start":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		batch, err := a.store.SetBatchRunning(ctx, batchID, true)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		a.scheduler.wake()
		writeJSON(w, http.StatusOK, batch)
		return

	case "stop":
		if r.Method != http.MethodPost {
			writeMethodNotAllowed(w)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		batch, err := a.store.SetBatchRunning(ctx, batchID, false)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, batch)
		return

	case "pins":
		if r.Method != http.MethodGet {
			writeMethodNotAllowed(w)
			return
		}
		mode := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("mode")))
		if mode == "" {
			mode = "all"
		}
		limit := 800
		if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
			if v, convErr := strconv.Atoi(raw); convErr == nil {
				limit = v
			}
		}

		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		items, err := a.store.ListBatchPins(ctx, batchID, mode, limit)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"items": items})
		return

	default:
		writeError(w, http.StatusNotFound, "not found")
		return
	}
}

func (a *API) handleIndexPage(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	a.serveWebText(w, http.StatusOK, "index.html", "text/html; charset=utf-8")
}

func (a *API) handleBatchPage(w http.ResponseWriter, r *http.Request) {
	a.serveWebText(w, http.StatusOK, "batch.html", "text/html; charset=utf-8")
}

func (a *API) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		log.Printf("%s %s (%s)", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
	})
}

func decodeJSON(r *http.Request, dst any) error {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return fmt.Errorf("request body must contain a single JSON object")
		}
		return err
	}
	return nil
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]any{"error": message})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func (a *API) serveWebText(w http.ResponseWriter, status int, name, contentType string) {
	body, err := fs.ReadFile(a.webFS, name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
