package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultConcurrency          = 3
	defaultHeartThreshold       = 2
	legacyDefaultHeartThreshold = 100
	defaultBatchMaxImages       = 100
	defaultBatchMaxDepth        = 3
	maxBatchMaxImages           = 5000
	maxBatchMaxDepth            = 20
	maxTaskAttempts             = 4
	sqliteBusyRetryMax          = 8
)

type Settings struct {
	Concurrency           int    `json:"concurrency"`
	GlobalRunning         bool   `json:"global_running"`
	DefaultThreshold      int    `json:"default_threshold"`
	DefaultBatchMaxImages int    `json:"default_batch_max_images"`
	ProxyType             string `json:"proxy_type"`
	ProxyHost             string `json:"proxy_host"`
	ProxyPort             int    `json:"proxy_port"`
	ProxyUsername         string `json:"proxy_username"`
	ProxyPassword         string `json:"proxy_password"`
}

type ProxyConfig struct {
	Type     string
	Host     string
	Port     int
	Username string
	Password string
}

func (s Settings) ProxyConfig() ProxyConfig {
	return ProxyConfig{
		Type:     strings.TrimSpace(s.ProxyType),
		Host:     strings.TrimSpace(s.ProxyHost),
		Port:     s.ProxyPort,
		Username: strings.TrimSpace(s.ProxyUsername),
		Password: s.ProxyPassword,
	}
}

type Batch struct {
	ID         int64  `json:"id"`
	Name       string `json:"name"`
	SeedURL    string `json:"seed_url"`
	Threshold  int    `json:"threshold"`
	MaxImages  int    `json:"max_images"`
	MaxDepth   int    `json:"max_depth"`
	IsRunning  bool   `json:"is_running"`
	CreatedAt  string `json:"created_at"`
	UpdatedAt  string `json:"updated_at"`
	Discovered int    `json:"discovered"`
	Scanned    int    `json:"scanned"`
	Saved      int    `json:"saved"`
	Pending    int    `json:"pending"`
	TopHearts  int    `json:"top_hearts"`
	CoverPath  string `json:"cover_path"`
	LastError  string `json:"last_error,omitempty"`
	CoverURL   string `json:"cover_url"`
}

type BatchPin struct {
	PinID         int64  `json:"pin_id"`
	URL           string `json:"url"`
	Hearts        int    `json:"hearts"`
	RelationLevel int    `json:"relation_level"`
	ImageURL      string `json:"image_url"`
	ImagePath     string `json:"image_path"`
	ImageViewURL  string `json:"image_view_url"`
	Title         string `json:"title"`
	LastCheckedAt string `json:"last_checked_at"`
	Included      bool   `json:"included"`
	Downloaded    bool   `json:"downloaded"`
}

type CrawlTask struct {
	ID        int64
	BatchID   int64
	URL       string
	Attempts  int
	Threshold int
	MaxImages int
	MaxDepth  int
	Depth     int
	Hearts    int
	CheckedAt string
}

type PinRecord struct {
	ID            int64
	URL           string
	Hearts        int
	ImageURL      string
	ImagePath     string
	Downloaded    bool
	Title         string
	LastCheckedAt string
}

type RuntimeStats struct {
	TotalPins      int `json:"total_pins"`
	DownloadedPins int `json:"downloaded_pins"`
	PendingTasks   int `json:"pending_tasks"`
	DoingTasks     int `json:"doing_tasks"`
	ErrorTasks     int `json:"error_tasks"`
	DoneTasks      int `json:"done_tasks"`
	RunningBatches int `json:"running_batches"`
}

type Store struct {
	db       *sql.DB
	imageDir string
	claimMu  sync.Mutex
}

const batchRetentionKeepURLsQuery = `
WITH task_candidates AS (
	SELECT
		t.url,
		CASE WHEN t.depth <= 0 THEN 1 ELSE t.depth END AS relation_level,
		t.id AS task_id,
		t.hearts
	FROM crawl_tasks t
	WHERE t.batch_id = ?
	UNION ALL
	SELECT
		ta.url,
		CASE WHEN ta.depth <= 0 THEN 1 ELSE ta.depth END AS relation_level,
		ta.task_id AS task_id,
		ta.hearts
	FROM crawl_tasks_archive ta
	WHERE ta.batch_id = ?
),
first_task AS (
	SELECT url, relation_level, task_id
	FROM (
		SELECT
			url,
			relation_level,
			task_id,
			ROW_NUMBER() OVER (
				PARTITION BY url
				ORDER BY relation_level ASC, task_id ASC
			) AS rn
		FROM task_candidates
	) ranked
	WHERE rn = 1
),
url_hearts AS (
	SELECT url, MAX(hearts) AS hearts
	FROM (
		SELECT url, hearts FROM task_candidates
		UNION ALL
		SELECT p.url, bp.hearts_snapshot
		FROM batch_pins bp
		JOIN pins p ON p.id = bp.pin_id
		WHERE bp.batch_id = ?
	) merged
	GROUP BY url
),
pin_state AS (
	SELECT
		p.url,
		MAX(bp.included) AS included
	FROM batch_pins bp
	JOIN pins p ON p.id = bp.pin_id
	WHERE bp.batch_id = ?
	GROUP BY p.url
),
url_pool AS (
	SELECT
		COALESCE(ft.url, ps.url) AS url,
		COALESCE(ft.relation_level, 999999) AS relation_level,
		COALESCE(ft.task_id, 9223372036854775807) AS task_id,
		COALESCE(uh.hearts, 0) AS hearts,
		COALESCE(ps.included, 0) AS included
	FROM first_task ft
	LEFT JOIN pin_state ps ON ps.url = ft.url
	LEFT JOIN url_hearts uh ON uh.url = ft.url
	UNION
	SELECT
		ps.url,
		999999 AS relation_level,
		9223372036854775807 AS task_id,
		COALESCE(uh.hearts, 0) AS hearts,
		ps.included
	FROM pin_state ps
	LEFT JOIN first_task ft ON ft.url = ps.url
	LEFT JOIN url_hearts uh ON uh.url = ps.url
	WHERE ft.url IS NULL
),
base_keep AS (
	SELECT url
	FROM url_pool
	WHERE included = 1 OR relation_level = 1
),
base_count AS (
	SELECT COUNT(1) AS total
	FROM base_keep
),
extra_keep AS (
	SELECT up.url
	FROM url_pool up
	WHERE up.relation_level = 2
		AND NOT EXISTS (
			SELECT 1
			FROM base_keep bk
			WHERE bk.url = up.url
		)
	ORDER BY up.hearts DESC, up.task_id ASC, up.url ASC
	LIMIT CASE
		WHEN ? > (SELECT total FROM base_count) THEN ? - (SELECT total FROM base_count)
		ELSE 0
	END
)
SELECT url FROM base_keep
UNION
SELECT url FROM extra_keep
`

func NewStore(db *sql.DB, imageDir string) (*Store, error) {
	s := &Store{db: db, imageDir: imageDir}
	if err := s.initSchema(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) initSchema() error {
	stmts := []string{
		`PRAGMA journal_mode = WAL;`,
		`PRAGMA busy_timeout = 5000;`,
		`PRAGMA foreign_keys = ON;`,
		`CREATE TABLE IF NOT EXISTS settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS batches (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			name TEXT NOT NULL,
			seed_url TEXT NOT NULL,
			threshold INTEGER NOT NULL DEFAULT 100,
			max_images INTEGER NOT NULL DEFAULT 100,
			max_depth INTEGER NOT NULL DEFAULT 3,
			is_running INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS pins (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			url TEXT NOT NULL UNIQUE,
			hearts INTEGER NOT NULL DEFAULT 0,
			image_url TEXT,
			image_path TEXT,
			downloaded INTEGER NOT NULL DEFAULT 0,
			title TEXT,
			last_checked_at TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS batch_pins (
			batch_id INTEGER NOT NULL,
			pin_id INTEGER NOT NULL,
			hearts_snapshot INTEGER NOT NULL DEFAULT 0,
			included INTEGER NOT NULL DEFAULT 0,
			discovered_at TEXT NOT NULL,
			PRIMARY KEY(batch_id, pin_id),
			FOREIGN KEY(batch_id) REFERENCES batches(id) ON DELETE CASCADE,
			FOREIGN KEY(pin_id) REFERENCES pins(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS crawl_tasks (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			batch_id INTEGER NOT NULL,
			url TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			attempts INTEGER NOT NULL DEFAULT 0,
			hearts INTEGER NOT NULL DEFAULT 0,
			checked_at TEXT,
			depth INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(batch_id, url),
			FOREIGN KEY(batch_id) REFERENCES batches(id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS crawl_tasks_archive (
			task_id INTEGER PRIMARY KEY,
			batch_id INTEGER NOT NULL,
			url TEXT NOT NULL,
			status TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			hearts INTEGER NOT NULL DEFAULT 0,
			checked_at TEXT,
			depth INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			archived_at TEXT NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_status_batch ON crawl_tasks(status, batch_id, id);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_batch ON crawl_tasks(batch_id);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_batch_checked ON crawl_tasks(batch_id, checked_at);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_batch_status_updated ON crawl_tasks(batch_id, status, updated_at DESC, id DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_batch_url_depth_id ON crawl_tasks(batch_id, url, depth, id);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_url ON crawl_tasks(url);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_archive_batch ON crawl_tasks_archive(batch_id);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_archive_archived_at ON crawl_tasks_archive(archived_at DESC);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_archive_batch_url_depth_task ON crawl_tasks_archive(batch_id, url, depth, task_id);`,
		`CREATE INDEX IF NOT EXISTS idx_tasks_archive_url ON crawl_tasks_archive(url);`,
		`CREATE INDEX IF NOT EXISTS idx_batch_pins_batch ON batch_pins(batch_id, included);`,
		`CREATE INDEX IF NOT EXISTS idx_batch_pins_batch_cover ON batch_pins(batch_id, included, discovered_at, pin_id);`,
		`CREATE INDEX IF NOT EXISTS idx_batch_pins_batch_hearts ON batch_pins(batch_id, hearts_snapshot DESC, pin_id);`,
		`CREATE INDEX IF NOT EXISTS idx_batch_pins_pin ON batch_pins(pin_id, batch_id);`,
		`CREATE INDEX IF NOT EXISTS idx_pins_hearts ON pins(hearts DESC);`,
	}

	for _, stmt := range stmts {
		if _, err := s.execWithRetry(context.Background(), stmt); err != nil {
			return fmt.Errorf("schema exec failed: %w", err)
		}
	}

	if _, err := s.execWithRetry(context.Background(), `ALTER TABLE batches ADD COLUMN max_images INTEGER NOT NULL DEFAULT 100`); err != nil {
		lower := strings.ToLower(err.Error())
		if !strings.Contains(lower, "duplicate column name") {
			return fmt.Errorf("migrate batches.max_images: %w", err)
		}
	}
	if _, err := s.execWithRetry(context.Background(), `ALTER TABLE batches ADD COLUMN max_depth INTEGER NOT NULL DEFAULT 3`); err != nil {
		lower := strings.ToLower(err.Error())
		if !strings.Contains(lower, "duplicate column name") {
			return fmt.Errorf("migrate batches.max_depth: %w", err)
		}
	}

	if _, err := s.execWithRetry(context.Background(), `ALTER TABLE crawl_tasks ADD COLUMN hearts INTEGER NOT NULL DEFAULT 0`); err != nil {
		lower := strings.ToLower(err.Error())
		if !strings.Contains(lower, "duplicate column name") {
			return fmt.Errorf("migrate crawl_tasks.hearts: %w", err)
		}
	}
	if _, err := s.execWithRetry(context.Background(), `ALTER TABLE crawl_tasks ADD COLUMN checked_at TEXT`); err != nil {
		lower := strings.ToLower(err.Error())
		if !strings.Contains(lower, "duplicate column name") {
			return fmt.Errorf("migrate crawl_tasks.checked_at: %w", err)
		}
	}
	if _, err := s.execWithRetry(context.Background(), `ALTER TABLE crawl_tasks ADD COLUMN depth INTEGER NOT NULL DEFAULT 0`); err != nil {
		lower := strings.ToLower(err.Error())
		if !strings.Contains(lower, "duplicate column name") {
			return fmt.Errorf("migrate crawl_tasks.depth: %w", err)
		}
	}

	if _, err := s.execWithRetry(context.Background(), `UPDATE batches SET max_images=? WHERE max_images <= 0`, defaultBatchMaxImages); err != nil {
		return fmt.Errorf("normalize batches.max_images: %w", err)
	}
	if _, err := s.execWithRetry(context.Background(), `UPDATE batches SET max_depth=? WHERE max_depth <= 0`, defaultBatchMaxDepth); err != nil {
		return fmt.Errorf("normalize batches.max_depth: %w", err)
	}
	if _, err := s.execWithRetry(context.Background(), `UPDATE batches SET max_depth=? WHERE max_depth > ?`, maxBatchMaxDepth, maxBatchMaxDepth); err != nil {
		return fmt.Errorf("normalize batches.max_depth upper: %w", err)
	}
	if _, err := s.execWithRetry(context.Background(), `UPDATE crawl_tasks SET hearts=0 WHERE hearts < 0`); err != nil {
		return fmt.Errorf("normalize crawl_tasks.hearts: %w", err)
	}
	if _, err := s.execWithRetry(context.Background(), `UPDATE crawl_tasks SET depth=0 WHERE depth < 0`); err != nil {
		return fmt.Errorf("normalize crawl_tasks.depth: %w", err)
	}
	if _, err := s.execWithRetry(context.Background(), `UPDATE crawl_tasks SET depth=1 WHERE depth=0`); err != nil {
		return fmt.Errorf("normalize crawl_tasks.depth baseline: %w", err)
	}
	if _, err := s.execWithRetry(context.Background(), `
		UPDATE crawl_tasks
		SET depth=0
		WHERE EXISTS (
			SELECT 1
			FROM batches b
			WHERE b.id = crawl_tasks.batch_id
				AND b.seed_url = crawl_tasks.url
		)
	`); err != nil {
		return fmt.Errorf("normalize crawl_tasks.depth seed: %w", err)
	}
	if _, err := s.execWithRetry(context.Background(), `CREATE INDEX IF NOT EXISTS idx_tasks_status_batch_depth ON crawl_tasks(status, batch_id, depth, id)`); err != nil {
		return fmt.Errorf("create crawl_tasks depth index: %w", err)
	}

	if err := s.ensureSetting("concurrency", fmt.Sprintf("%d", defaultConcurrency)); err != nil {
		return err
	}
	if err := s.ensureSetting("global_running", "0"); err != nil {
		return err
	}
	if err := s.ensureSetting("default_threshold", fmt.Sprintf("%d", defaultHeartThreshold)); err != nil {
		return err
	}
	if err := s.ensureSetting("default_batch_max_images", fmt.Sprintf("%d", defaultBatchMaxImages)); err != nil {
		return err
	}
	if err := s.ensureSetting("proxy_type", ""); err != nil {
		return err
	}
	if err := s.ensureSetting("proxy_host", ""); err != nil {
		return err
	}
	if err := s.ensureSetting("proxy_port", "0"); err != nil {
		return err
	}
	if err := s.ensureSetting("proxy_username", ""); err != nil {
		return err
	}
	if err := s.ensureSetting("proxy_password", ""); err != nil {
		return err
	}
	if _, err := s.execWithRetry(
		context.Background(),
		`UPDATE settings SET value=? WHERE key='default_threshold' AND TRIM(value)=?`,
		fmt.Sprintf("%d", defaultHeartThreshold),
		fmt.Sprintf("%d", legacyDefaultHeartThreshold),
	); err != nil {
		return fmt.Errorf("migrate default threshold: %w", err)
	}

	if _, err := s.execWithRetry(context.Background(), `UPDATE crawl_tasks SET status='pending' WHERE status='processing'`); err != nil {
		return fmt.Errorf("reset processing tasks: %w", err)
	}

	return nil
}

func (s *Store) ensureSetting(key, value string) error {
	_, err := s.execWithRetry(context.Background(), `INSERT INTO settings(key, value) VALUES(?, ?) ON CONFLICT(key) DO NOTHING`, key, value)
	if err != nil {
		return fmt.Errorf("ensure setting %s: %w", key, err)
	}
	return nil
}

func (s *Store) setSetting(key, value string) error {
	_, err := s.execWithRetry(context.Background(), `
		INSERT INTO settings(key, value)
		VALUES(?, ?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value
	`, key, value)
	if err != nil {
		return fmt.Errorf("set setting %s: %w", key, err)
	}
	return nil
}

func (s *Store) GetSettings() (Settings, error) {
	concurrency, err := s.getSettingInt("concurrency", defaultConcurrency)
	if err != nil {
		return Settings{}, err
	}
	globalRunning, err := s.getSettingInt("global_running", 0)
	if err != nil {
		return Settings{}, err
	}
	defaultThreshold, err := s.getSettingInt("default_threshold", defaultHeartThreshold)
	if err != nil {
		return Settings{}, err
	}
	defaultBatchLimit, err := s.getSettingInt("default_batch_max_images", defaultBatchMaxImages)
	if err != nil {
		return Settings{}, err
	}
	proxyType, err := s.getSettingString("proxy_type", "")
	if err != nil {
		return Settings{}, err
	}
	proxyHost, err := s.getSettingString("proxy_host", "")
	if err != nil {
		return Settings{}, err
	}
	proxyPort, err := s.getSettingInt("proxy_port", 0)
	if err != nil {
		return Settings{}, err
	}
	proxyUsername, err := s.getSettingString("proxy_username", "")
	if err != nil {
		return Settings{}, err
	}
	proxyPassword, err := s.getSettingString("proxy_password", "")
	if err != nil {
		return Settings{}, err
	}
	return Settings{
		Concurrency:           clampConcurrency(concurrency),
		GlobalRunning:         globalRunning == 1,
		DefaultThreshold:      defaultThreshold,
		DefaultBatchMaxImages: clampBatchMaxImages(defaultBatchLimit),
		ProxyType:             proxyType,
		ProxyHost:             proxyHost,
		ProxyPort:             proxyPort,
		ProxyUsername:         proxyUsername,
		ProxyPassword:         proxyPassword,
	}, nil
}

func (s *Store) getSettingInt(key string, fallback int) (int, error) {
	row := s.db.QueryRow(`SELECT value FROM settings WHERE key=?`, key)
	var raw string
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fallback, nil
		}
		return 0, fmt.Errorf("read setting %s: %w", key, err)
	}
	value := parseCount(raw)
	if value <= 0 {
		return fallback, nil
	}
	return value, nil
}

func (s *Store) getSettingString(key string, fallback string) (string, error) {
	row := s.db.QueryRow(`SELECT value FROM settings WHERE key=?`, key)
	var raw string
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fallback, nil
		}
		return "", fmt.Errorf("read setting %s: %w", key, err)
	}
	return raw, nil
}

func (s *Store) UpdateConcurrency(value int) error {
	value = clampConcurrency(value)
	return s.setSetting("concurrency", fmt.Sprintf("%d", value))
}

func (s *Store) SetGlobalRunning(running bool) error {
	return s.setSetting("global_running", fmt.Sprintf("%d", boolToInt(running)))
}

func (s *Store) UpdateDefaultBatchMaxImages(value int) error {
	value = clampBatchMaxImages(value)
	return s.setSetting("default_batch_max_images", fmt.Sprintf("%d", value))
}

func (s *Store) UpdateProxySettings(config ProxyConfig) error {
	if err := s.setSetting("proxy_type", config.Type); err != nil {
		return err
	}
	if err := s.setSetting("proxy_host", config.Host); err != nil {
		return err
	}
	if err := s.setSetting("proxy_port", fmt.Sprintf("%d", config.Port)); err != nil {
		return err
	}
	if err := s.setSetting("proxy_username", config.Username); err != nil {
		return err
	}
	if err := s.setSetting("proxy_password", config.Password); err != nil {
		return err
	}
	return nil
}

func (s *Store) CreateBatch(ctx context.Context, name, seedURL string, threshold, maxImages, maxDepth int) (Batch, error) {
	seedURL, err := normalizePinURL(seedURL)
	if err != nil {
		return Batch{}, fmt.Errorf("invalid seed url: %w", err)
	}

	settings, err := s.GetSettings()
	if err != nil {
		return Batch{}, err
	}

	if strings.TrimSpace(name) == "" {
		name = "Batch " + timeKey()
	}

	if threshold <= 0 {
		threshold = settings.DefaultThreshold
	}
	if maxImages <= 0 {
		maxImages = settings.DefaultBatchMaxImages
	}
	if maxDepth <= 0 {
		maxDepth = defaultBatchMaxDepth
	}
	maxImages = clampBatchMaxImages(maxImages)
	maxDepth = clampBatchMaxDepth(maxDepth)

	now := nowISO()
	res, err := s.execWithRetry(ctx,
		`INSERT INTO batches(name, seed_url, threshold, max_images, max_depth, is_running, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?)`,
		name, seedURL, threshold, maxImages, maxDepth, now, now,
	)
	if err != nil {
		return Batch{}, fmt.Errorf("create batch: %w", err)
	}

	id, err := res.LastInsertId()
	if err != nil {
		return Batch{}, fmt.Errorf("batch id: %w", err)
	}

	if err := s.EnqueueTask(ctx, id, seedURL, 0); err != nil {
		return Batch{}, fmt.Errorf("seed enqueue: %w", err)
	}

	return s.GetBatch(ctx, id)
}

func (s *Store) ListBatches(ctx context.Context) ([]Batch, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			b.id,
			b.name,
			b.seed_url,
			b.threshold,
			b.max_images,
			b.max_depth,
			b.is_running,
			b.created_at,
			b.updated_at,
			COALESCE(task_agg.discovered, 0) + COALESCE(task_archive_agg.discovered, 0) AS discovered,
			COALESCE(task_agg.scanned, 0) + COALESCE(task_archive_agg.scanned, 0) AS scanned,
			COALESCE(pin_agg.saved, 0) AS saved,
			COALESCE(task_agg.pending, 0) AS pending,
			COALESCE(pin_agg.top_hearts, 0) AS top_hearts,
			COALESCE((SELECT p.image_path
				FROM batch_pins bp
				JOIN pins p ON p.id = bp.pin_id
				WHERE bp.batch_id = b.id AND bp.included = 1 AND p.downloaded = 1
				ORDER BY bp.discovered_at ASC, p.id ASC
				LIMIT 1), '') AS cover_path,
			COALESCE((SELECT t.last_error
				FROM crawl_tasks t
				WHERE t.batch_id = b.id AND t.status = 'error'
				ORDER BY t.updated_at DESC
				LIMIT 1), '') AS last_error
		FROM batches b
		LEFT JOIN (
			SELECT
				t.batch_id,
				COUNT(1) AS discovered,
				SUM(CASE WHEN t.checked_at IS NOT NULL THEN 1 ELSE 0 END) AS scanned,
				SUM(CASE WHEN t.status = 'pending' THEN 1 ELSE 0 END) AS pending
			FROM crawl_tasks t
			GROUP BY t.batch_id
		) AS task_agg ON task_agg.batch_id = b.id
		LEFT JOIN (
			SELECT
				ta.batch_id,
				COUNT(1) AS discovered,
				SUM(CASE WHEN ta.checked_at IS NOT NULL THEN 1 ELSE 0 END) AS scanned
			FROM crawl_tasks_archive ta
			GROUP BY ta.batch_id
		) AS task_archive_agg ON task_archive_agg.batch_id = b.id
		LEFT JOIN (
			SELECT
				bp.batch_id,
				SUM(CASE WHEN bp.included = 1 THEN 1 ELSE 0 END) AS saved,
				MAX(bp.hearts_snapshot) AS top_hearts
			FROM batch_pins bp
			GROUP BY bp.batch_id
		) AS pin_agg ON pin_agg.batch_id = b.id
		ORDER BY COALESCE(pin_agg.top_hearts, 0) DESC, b.created_at DESC, b.id DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("list batches: %w", err)
	}
	defer rows.Close()

	items := make([]Batch, 0)
	for rows.Next() {
		var item Batch
		var running int
		if err := rows.Scan(
			&item.ID,
			&item.Name,
			&item.SeedURL,
			&item.Threshold,
			&item.MaxImages,
			&item.MaxDepth,
			&running,
			&item.CreatedAt,
			&item.UpdatedAt,
			&item.Discovered,
			&item.Scanned,
			&item.Saved,
			&item.Pending,
			&item.TopHearts,
			&item.CoverPath,
			&item.LastError,
		); err != nil {
			return nil, fmt.Errorf("scan batch: %w", err)
		}
		item.IsRunning = intToBool(running)
		item.CoverURL = toWebImageURL(item.CoverPath)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) GetBatch(ctx context.Context, batchID int64) (Batch, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT
			b.id,
			b.name,
			b.seed_url,
			b.threshold,
			b.max_images,
			b.max_depth,
			b.is_running,
			b.created_at,
			b.updated_at,
			COALESCE((SELECT COUNT(1) FROM crawl_tasks t WHERE t.batch_id = b.id), 0) +
				COALESCE((SELECT COUNT(1) FROM crawl_tasks_archive ta WHERE ta.batch_id = b.id), 0) AS discovered,
			COALESCE((SELECT COUNT(1) FROM crawl_tasks t WHERE t.batch_id = b.id AND t.checked_at IS NOT NULL), 0) +
				COALESCE((SELECT COUNT(1) FROM crawl_tasks_archive ta WHERE ta.batch_id = b.id AND ta.checked_at IS NOT NULL), 0) AS scanned,
			COALESCE((SELECT COUNT(1) FROM batch_pins bp WHERE bp.batch_id = b.id AND bp.included = 1), 0) AS saved,
			COALESCE((SELECT COUNT(1) FROM crawl_tasks t WHERE t.batch_id = b.id AND t.status = 'pending'), 0) AS pending,
			COALESCE((SELECT MAX(bp.hearts_snapshot) FROM batch_pins bp WHERE bp.batch_id = b.id), 0) AS top_hearts,
			COALESCE((SELECT p.image_path
				FROM batch_pins bp
				JOIN pins p ON p.id = bp.pin_id
				WHERE bp.batch_id = b.id AND bp.included = 1 AND p.downloaded = 1
				ORDER BY bp.discovered_at ASC, p.id ASC
				LIMIT 1), '') AS cover_path,
			COALESCE((SELECT t.last_error
				FROM crawl_tasks t
				WHERE t.batch_id = b.id AND t.status = 'error'
				ORDER BY t.updated_at DESC
				LIMIT 1), '') AS last_error
		FROM batches b
		WHERE b.id = ?
	`, batchID)

	var item Batch
	var running int
	if err := row.Scan(
		&item.ID,
		&item.Name,
		&item.SeedURL,
		&item.Threshold,
		&item.MaxImages,
		&item.MaxDepth,
		&running,
		&item.CreatedAt,
		&item.UpdatedAt,
		&item.Discovered,
		&item.Scanned,
		&item.Saved,
		&item.Pending,
		&item.TopHearts,
		&item.CoverPath,
		&item.LastError,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Batch{}, fmt.Errorf("batch not found")
		}
		return Batch{}, fmt.Errorf("get batch: %w", err)
	}
	item.IsRunning = intToBool(running)
	item.CoverURL = toWebImageURL(item.CoverPath)
	return item, nil
}

func (s *Store) SetBatchRunning(ctx context.Context, batchID int64, running bool) (Batch, error) {
	_, err := s.execWithRetry(ctx, `UPDATE batches SET is_running=?, updated_at=? WHERE id=?`, boolToInt(running), nowISO(), batchID)
	if err != nil {
		return Batch{}, fmt.Errorf("set batch running: %w", err)
	}
	if running {
		var seed string
		if err := s.db.QueryRowContext(ctx, `SELECT seed_url FROM batches WHERE id=?`, batchID).Scan(&seed); err == nil {
			_ = s.EnqueueTask(ctx, batchID, seed, 0)
		}
	} else {
		if _, err := s.FinalizeBatchIfReady(ctx, batchID); err != nil {
			return Batch{}, fmt.Errorf("compact stopped batch: %w", err)
		}
	}
	return s.GetBatch(ctx, batchID)
}

func (s *Store) UpdateBatchRules(ctx context.Context, batchID int64, threshold *int, maxDepth *int) (Batch, error) {
	if threshold == nil && maxDepth == nil {
		return Batch{}, fmt.Errorf("at least one rule is required")
	}

	setParts := make([]string, 0, 3)
	args := make([]any, 0, 4)
	if threshold != nil {
		if *threshold <= 0 {
			return Batch{}, fmt.Errorf("threshold must be >= 1")
		}
		setParts = append(setParts, "threshold=?")
		args = append(args, *threshold)
	}
	if maxDepth != nil {
		if *maxDepth <= 0 {
			return Batch{}, fmt.Errorf("max_depth must be >= 1")
		}
		clampedDepth := clampBatchMaxDepth(*maxDepth)
		setParts = append(setParts, "max_depth=?")
		args = append(args, clampedDepth)
	}

	setParts = append(setParts, "updated_at=?")
	args = append(args, nowISO(), batchID)
	query := fmt.Sprintf("UPDATE batches SET %s WHERE id=?", strings.Join(setParts, ", "))

	res, err := s.execWithRetry(ctx, query, args...)
	if err != nil {
		return Batch{}, fmt.Errorf("update batch rules: %w", err)
	}

	affected, err := res.RowsAffected()
	if err == nil && affected == 0 {
		return Batch{}, fmt.Errorf("batch not found")
	}

	return s.GetBatch(ctx, batchID)
}

func (s *Store) UpdateBatchThreshold(ctx context.Context, batchID int64, threshold int) (Batch, error) {
	return s.UpdateBatchRules(ctx, batchID, &threshold, nil)
}

func (s *Store) DeleteBatch(ctx context.Context, batchID int64) (int, error) {
	tx, err := s.beginTxWithRetry(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin delete batch tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var exists int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM batches WHERE id=?`, batchID).Scan(&exists); err != nil {
		return 0, fmt.Errorf("check batch exists: %w", err)
	}
	if exists == 0 {
		return 0, fmt.Errorf("batch not found")
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT DISTINCT p.id, COALESCE(p.image_path, '')
		FROM batch_pins bp
		JOIN pins p ON p.id = bp.pin_id
		WHERE bp.batch_id = ? AND bp.included = 1 AND p.downloaded = 1 AND COALESCE(p.image_path, '') <> ''
	`, batchID)
	if err != nil {
		return 0, fmt.Errorf("list batch images before delete: %w", err)
	}

	candidateImagePaths := make(map[int64]string, 128)
	for rows.Next() {
		var pinID int64
		var imagePath string
		if err := rows.Scan(&pinID, &imagePath); err != nil {
			_ = rows.Close()
			return 0, fmt.Errorf("scan batch image before delete: %w", err)
		}
		candidateImagePaths[pinID] = imagePath
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return 0, fmt.Errorf("iterate batch images before delete: %w", err)
	}
	_ = rows.Close()

	if _, err := s.execTxWithRetry(ctx, tx, `DELETE FROM batches WHERE id=?`, batchID); err != nil {
		return 0, fmt.Errorf("delete batch: %w", err)
	}

	orphanPinIDs := make([]int64, 0, len(candidateImagePaths))
	orphanImagePaths := make([]string, 0, len(candidateImagePaths))
	for pinID, imagePath := range candidateImagePaths {
		var refs int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM batch_pins WHERE pin_id=? AND included=1`, pinID).Scan(&refs); err != nil {
			return 0, fmt.Errorf("check remaining batch pin refs: %w", err)
		}
		if refs == 0 {
			orphanPinIDs = append(orphanPinIDs, pinID)
			orphanImagePaths = append(orphanImagePaths, imagePath)
		}
	}

	if len(orphanPinIDs) > 0 {
		placeholders := strings.TrimRight(strings.Repeat("?,", len(orphanPinIDs)), ",")
		args := make([]any, 0, len(orphanPinIDs)+1)
		args = append(args, nowISO())
		for _, pinID := range orphanPinIDs {
			args = append(args, pinID)
		}

		query := fmt.Sprintf(`UPDATE pins SET downloaded=0, image_path='', updated_at=? WHERE id IN (%s)`, placeholders)
		if _, err := s.execTxWithRetry(ctx, tx, query, args...); err != nil {
			return 0, fmt.Errorf("clear orphan pin image paths: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit delete batch tx: %w", err)
	}

	deletedImages := 0
	for _, imagePath := range orphanImagePaths {
		imagePath = strings.TrimSpace(imagePath)
		if imagePath == "" {
			continue
		}
		if err := os.Remove(imagePath); err == nil {
			deletedImages++
		} else if !errors.Is(err, os.ErrNotExist) {
			// Best effort cleanup; keep DB deletion successful even if file delete fails.
			continue
		}
	}

	return deletedImages, nil
}

func (s *Store) ArchiveDoneTasks(ctx context.Context, limit int) (int, error) {
	if limit <= 0 || limit > 50000 {
		limit = 10000
	}

	tx, err := s.beginTxWithRetry(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin archive tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	archivedAt := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.execTxWithRetry(ctx, tx, `
		INSERT INTO crawl_tasks_archive(
			task_id,
			batch_id,
			url,
			status,
			attempts,
			hearts,
			checked_at,
			depth,
			last_error,
			created_at,
			updated_at,
			archived_at
		)
		SELECT
			t.id,
			t.batch_id,
			t.url,
			t.status,
			t.attempts,
			t.hearts,
			t.checked_at,
			t.depth,
			COALESCE(t.last_error, ''),
			t.created_at,
			t.updated_at,
			?
		FROM crawl_tasks t
		JOIN batches b ON b.id = t.batch_id
		WHERE t.status = 'done' AND b.is_running = 0
		ORDER BY t.id ASC
		LIMIT ?
	`, archivedAt, limit)
	if err != nil {
		return 0, fmt.Errorf("archive done tasks insert: %w", err)
	}

	inserted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("archive done tasks inserted rows: %w", err)
	}
	if inserted <= 0 {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit empty archive tx: %w", err)
		}
		return 0, nil
	}

	deleteRes, err := s.execTxWithRetry(ctx, tx, `
		DELETE FROM crawl_tasks
		WHERE id IN (
			SELECT ta.task_id
			FROM crawl_tasks_archive ta
			WHERE ta.archived_at = ?
		)
	`, archivedAt)
	if err != nil {
		return 0, fmt.Errorf("archive done tasks delete active: %w", err)
	}

	deleted, err := deleteRes.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("archive done tasks deleted rows: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit archive tx: %w", err)
	}

	return int(deleted), nil
}

func (s *Store) EnqueueTask(ctx context.Context, batchID int64, rawURL string, depth int) error {
	normalized, err := normalizePinURL(rawURL)
	if err != nil {
		return err
	}

	var knownCount int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(1)
		FROM batch_pins bp
		JOIN pins p ON p.id = bp.pin_id
		WHERE bp.batch_id = ? AND p.url = ?
	`, batchID, normalized).Scan(&knownCount); err == nil && knownCount > 0 {
		return nil
	}

	depth = clampTaskDepth(depth)
	now := nowISO()
	_, err = s.execWithRetry(ctx, `
		INSERT INTO crawl_tasks(batch_id, url, status, attempts, hearts, checked_at, depth, created_at, updated_at)
		VALUES (?, ?, 'pending', 0, 0, NULL, ?, ?, ?)
		ON CONFLICT(batch_id, url) DO UPDATE SET
			updated_at = excluded.updated_at,
			depth = CASE WHEN excluded.depth < crawl_tasks.depth THEN excluded.depth ELSE crawl_tasks.depth END,
			status = CASE WHEN crawl_tasks.status = 'error' THEN 'pending' ELSE crawl_tasks.status END
	`, batchID, normalized, depth, now, now)
	if err != nil {
		return fmt.Errorf("enqueue task: %w", err)
	}
	return nil
}

func (s *Store) EnqueueMany(ctx context.Context, batchID int64, rawURLs []string, depth int) error {
	if len(rawURLs) == 0 {
		return nil
	}
	depth = clampTaskDepth(depth)

	seen := make(map[string]struct{}, len(rawURLs))
	for _, item := range rawURLs {
		normalized, err := normalizePinURL(item)
		if err != nil {
			continue
		}
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		if err := s.EnqueueTask(ctx, batchID, normalized, depth); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ClaimPendingTask(ctx context.Context) (*CrawlTask, error) {
	s.claimMu.Lock()
	defer s.claimMu.Unlock()

	row := s.db.QueryRowContext(ctx, `
		SELECT t.id, t.batch_id, t.url, t.attempts, b.threshold, b.max_images, b.max_depth, t.depth, t.hearts, COALESCE(t.checked_at, '')
		FROM crawl_tasks t
		JOIN batches b ON b.id = t.batch_id
		WHERE t.status = 'pending' AND b.is_running = 1
		ORDER BY
			(
				SELECT COUNT(1)
				FROM crawl_tasks tp
				WHERE tp.batch_id = t.batch_id AND tp.status = 'processing'
			) ASC,
			t.depth ASC,
			t.id ASC
		LIMIT 1
	`)

	task := CrawlTask{}
	if err := row.Scan(&task.ID, &task.BatchID, &task.URL, &task.Attempts, &task.Threshold, &task.MaxImages, &task.MaxDepth, &task.Depth, &task.Hearts, &task.CheckedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("claim select: %w", err)
	}

	task.Attempts++
	_, err := s.execWithRetry(ctx, `
		UPDATE crawl_tasks
		SET status='processing', attempts=?, updated_at=?
		WHERE id=?
	`, task.Attempts, nowISO(), task.ID)
	if err != nil {
		return nil, fmt.Errorf("claim update: %w", err)
	}
	return &task, nil
}

func (s *Store) MarkTaskDone(ctx context.Context, taskID int64, hearts int) error {
	if hearts < 0 {
		hearts = 0
	}
	now := nowISO()
	_, err := s.execWithRetry(ctx, `
		UPDATE crawl_tasks
		SET status='done', hearts=?, checked_at=?, last_error='', updated_at=?
		WHERE id=?
	`, hearts, now, now, taskID)
	if err != nil {
		return fmt.Errorf("mark done: %w", err)
	}
	return nil
}

func (s *Store) MarkTaskFailure(ctx context.Context, task CrawlTask, taskErr error) error {
	status := "pending"
	if task.Attempts >= maxTaskAttempts {
		status = "error"
	}
	errText := "unknown"
	if taskErr != nil {
		errText = taskErr.Error()
	}
	if len(errText) > 400 {
		errText = errText[:400]
	}

	_, err := s.execWithRetry(ctx,
		`UPDATE crawl_tasks SET status=?, last_error=?, updated_at=? WHERE id=?`,
		status, errText, nowISO(), task.ID,
	)
	if err != nil {
		return fmt.Errorf("mark failure: %w", err)
	}
	return nil
}

func (s *Store) UpsertPin(ctx context.Context, result CrawlResult) (PinRecord, error) {
	normalizedURL, err := normalizePinURL(result.URL)
	if err != nil {
		return PinRecord{}, err
	}

	now := nowISO()
	_, err = s.execWithRetry(ctx, `
		INSERT INTO pins(url, hearts, image_url, image_path, downloaded, title, last_checked_at, created_at, updated_at)
		VALUES (?, ?, ?, '', 0, ?, ?, ?, ?)
		ON CONFLICT(url) DO UPDATE SET
			hearts = CASE WHEN excluded.hearts > pins.hearts THEN excluded.hearts ELSE pins.hearts END,
			image_url = CASE
				WHEN (pins.image_url IS NULL OR pins.image_url = '') AND excluded.image_url <> '' THEN excluded.image_url
				ELSE pins.image_url
			END,
			title = CASE
				WHEN (pins.title IS NULL OR pins.title = '') AND excluded.title <> '' THEN excluded.title
				ELSE pins.title
			END,
			last_checked_at = excluded.last_checked_at,
			updated_at = excluded.updated_at
	`, normalizedURL, result.Hearts, strings.TrimSpace(result.ImageURL), strings.TrimSpace(result.Title), now, now, now)
	if err != nil {
		return PinRecord{}, fmt.Errorf("upsert pin: %w", err)
	}

	row := s.db.QueryRowContext(ctx, `
		SELECT id, url, hearts, COALESCE(image_url, ''), COALESCE(image_path, ''), downloaded, COALESCE(title, ''), COALESCE(last_checked_at, '')
		FROM pins
		WHERE url = ?
	`, normalizedURL)

	var record PinRecord
	var downloaded int
	if err := row.Scan(
		&record.ID,
		&record.URL,
		&record.Hearts,
		&record.ImageURL,
		&record.ImagePath,
		&downloaded,
		&record.Title,
		&record.LastCheckedAt,
	); err != nil {
		return PinRecord{}, fmt.Errorf("read pin after upsert: %w", err)
	}
	record.Downloaded = intToBool(downloaded)
	return record, nil
}

func (s *Store) UpdatePinImage(ctx context.Context, pinID int64, imageURL, localPath string) error {
	_, err := s.execWithRetry(ctx, `
		UPDATE pins
		SET image_url = CASE WHEN image_url IS NULL OR image_url = '' THEN ? ELSE image_url END,
			image_path = ?,
			downloaded = 1,
			updated_at = ?
		WHERE id = ?
	`, strings.TrimSpace(imageURL), localPath, nowISO(), pinID)
	if err != nil {
		return fmt.Errorf("update pin image: %w", err)
	}
	return nil
}

func (s *Store) BatchReachedImageCap(ctx context.Context, batchID int64) (bool, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT
			b.max_images,
			COALESCE((
				SELECT COUNT(1)
				FROM batch_pins bp
				WHERE bp.batch_id = b.id AND bp.included = 1
			), 0) AS saved
		FROM batches b
		WHERE b.id = ?
	`, batchID)

	var maxImages int
	var saved int
	if err := row.Scan(&maxImages, &saved); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("batch not found")
		}
		return false, fmt.Errorf("check batch image cap: %w", err)
	}
	if maxImages <= 0 {
		return false, nil
	}
	return saved >= maxImages, nil
}

func (s *Store) FinalizeBatchIfReady(ctx context.Context, batchID int64) (bool, error) {
	tx, err := s.beginTxWithRetry(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin finalize batch tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var maxImages int
	var isRunning int
	var saved int
	if err := tx.QueryRowContext(ctx, `
		SELECT
			b.max_images,
			b.is_running,
			COALESCE((
				SELECT COUNT(1)
				FROM batch_pins bp
				WHERE bp.batch_id = b.id AND bp.included = 1
			), 0) AS saved
		FROM batches b
		WHERE b.id = ?
	`, batchID).Scan(&maxImages, &isRunning, &saved); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, fmt.Errorf("batch not found")
		}
		return false, fmt.Errorf("load batch finalize state: %w", err)
	}
	if maxImages <= 0 {
		return false, nil
	}
	if saved < maxImages && isRunning != 0 {
		return false, nil
	}

	for _, stmt := range []string{
		`CREATE TEMP TABLE IF NOT EXISTS temp_keep_urls(url TEXT PRIMARY KEY)`,
		`CREATE TEMP TABLE IF NOT EXISTS temp_removed_pins(
			pin_id INTEGER PRIMARY KEY,
			url TEXT NOT NULL,
			image_path TEXT NOT NULL
		)`,
		`CREATE TEMP TABLE IF NOT EXISTS temp_orphan_pins(
			pin_id INTEGER PRIMARY KEY,
			image_path TEXT NOT NULL
		)`,
		`DELETE FROM temp_keep_urls`,
		`DELETE FROM temp_removed_pins`,
		`DELETE FROM temp_orphan_pins`,
	} {
		if _, err := s.execTxWithRetry(ctx, tx, stmt); err != nil {
			return false, fmt.Errorf("prepare finalize temp tables: %w", err)
		}
	}

	if _, err := s.execTxWithRetry(ctx, tx,
		`INSERT OR IGNORE INTO temp_keep_urls(url) `+batchRetentionKeepURLsQuery,
		batchID, batchID, batchID, batchID, maxImages, maxImages,
	); err != nil {
		return false, fmt.Errorf("load batch retention urls: %w", err)
	}

	if _, err := s.execTxWithRetry(ctx, tx, `
		INSERT OR IGNORE INTO temp_removed_pins(pin_id, url, image_path)
		SELECT DISTINCT
			p.id,
			p.url,
			COALESCE(p.image_path, '')
		FROM batch_pins bp
		JOIN pins p ON p.id = bp.pin_id
		WHERE bp.batch_id = ?
			AND NOT EXISTS (
				SELECT 1
				FROM temp_keep_urls k
				WHERE k.url = p.url
			)
	`, batchID); err != nil {
		return false, fmt.Errorf("collect batch removed pins: %w", err)
	}

	if _, err := s.execTxWithRetry(ctx, tx, `
		DELETE FROM batch_pins
		WHERE batch_id = ?
			AND pin_id IN (
				SELECT p.id
				FROM pins p
				WHERE NOT EXISTS (
					SELECT 1
					FROM temp_keep_urls k
					WHERE k.url = p.url
				)
			)
	`, batchID); err != nil {
		return false, fmt.Errorf("delete pruned batch pins: %w", err)
	}

	if _, err := s.execTxWithRetry(ctx, tx, `
		DELETE FROM crawl_tasks
		WHERE batch_id = ?
			AND NOT EXISTS (
				SELECT 1
				FROM temp_keep_urls k
				WHERE k.url = crawl_tasks.url
			)
	`, batchID); err != nil {
		return false, fmt.Errorf("delete pruned crawl tasks: %w", err)
	}

	if _, err := s.execTxWithRetry(ctx, tx, `
		DELETE FROM crawl_tasks_archive
		WHERE batch_id = ?
			AND NOT EXISTS (
				SELECT 1
				FROM temp_keep_urls k
				WHERE k.url = crawl_tasks_archive.url
			)
	`, batchID); err != nil {
		return false, fmt.Errorf("delete pruned archived tasks: %w", err)
	}

	if _, err := s.execTxWithRetry(ctx, tx, `
		INSERT OR IGNORE INTO temp_orphan_pins(pin_id, image_path)
		SELECT
			p.id,
			COALESCE(p.image_path, '')
		FROM temp_removed_pins rp
		JOIN pins p ON p.id = rp.pin_id
		WHERE NOT EXISTS (
			SELECT 1
			FROM batch_pins bp
			WHERE bp.pin_id = p.id
		)
			AND NOT EXISTS (
				SELECT 1
				FROM crawl_tasks t
				WHERE t.url = p.url
			)
			AND NOT EXISTS (
				SELECT 1
				FROM crawl_tasks_archive ta
				WHERE ta.url = p.url
			)
	`); err != nil {
		return false, fmt.Errorf("collect orphan pins: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT image_path
		FROM temp_orphan_pins
		WHERE image_path <> ''
	`)
	if err != nil {
		return false, fmt.Errorf("list orphan image paths: %w", err)
	}

	imagePaths := make([]string, 0, 32)
	for rows.Next() {
		var imagePath string
		if err := rows.Scan(&imagePath); err != nil {
			_ = rows.Close()
			return false, fmt.Errorf("scan orphan image path: %w", err)
		}
		imagePaths = append(imagePaths, imagePath)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return false, fmt.Errorf("iterate orphan image paths: %w", err)
	}
	_ = rows.Close()

	if _, err := s.execTxWithRetry(ctx, tx, `
		DELETE FROM pins
		WHERE id IN (SELECT pin_id FROM temp_orphan_pins)
	`); err != nil {
		return false, fmt.Errorf("delete orphan pins: %w", err)
	}

	if _, err := s.execTxWithRetry(ctx, tx, `
		UPDATE batches
		SET is_running = 0, updated_at = ?
		WHERE id = ?
	`, nowISO(), batchID); err != nil {
		return false, fmt.Errorf("stop full batch: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit finalize batch tx: %w", err)
	}

	for _, imagePath := range imagePaths {
		imagePath = strings.TrimSpace(imagePath)
		if imagePath == "" {
			continue
		}
		if err := os.Remove(imagePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Printf("cleanup orphan image warning: %v", err)
		}
	}

	return true, nil
}

func (s *Store) CompactEligibleBatches(ctx context.Context) (int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT b.id
		FROM batches b
		WHERE b.is_running = 0
			OR COALESCE((
				SELECT COUNT(1)
				FROM batch_pins bp
				WHERE bp.batch_id = b.id AND bp.included = 1
			), 0) >= b.max_images
		ORDER BY b.id ASC
	`)
	if err != nil {
		return 0, fmt.Errorf("list eligible compact batches: %w", err)
	}
	defer rows.Close()

	batchIDs := make([]int64, 0, 16)
	for rows.Next() {
		var batchID int64
		if err := rows.Scan(&batchID); err != nil {
			return 0, fmt.Errorf("scan compact batch id: %w", err)
		}
		batchIDs = append(batchIDs, batchID)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	compacted := 0
	for _, batchID := range batchIDs {
		done, err := s.FinalizeBatchIfReady(ctx, batchID)
		if err != nil {
			return compacted, err
		}
		if done {
			compacted++
		}
	}
	return compacted, nil
}

func (s *Store) UpsertBatchPin(ctx context.Context, batchID, pinID int64, hearts int, qualified bool, maxImages int) (bool, error) {
	maxImages = clampBatchMaxImages(maxImages)

	tx, err := s.beginTxWithRetry(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin batch pin tx: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	var currentIncluded int
	err = tx.QueryRowContext(ctx, `SELECT included FROM batch_pins WHERE batch_id=? AND pin_id=?`, batchID, pinID).Scan(&currentIncluded)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("read batch pin included: %w", err)
	}

	included := false
	switch {
	case err == nil && currentIncluded == 1:
		included = true
	case !qualified:
		included = false
	default:
		var savedCount int
		if countErr := tx.QueryRowContext(ctx, `SELECT COUNT(1) FROM batch_pins WHERE batch_id=? AND included=1`, batchID).Scan(&savedCount); countErr != nil {
			return false, fmt.Errorf("count included batch pins: %w", countErr)
		}
		included = savedCount < maxImages
	}

	_, err = s.execTxWithRetry(ctx, tx, `
		INSERT INTO batch_pins(batch_id, pin_id, hearts_snapshot, included, discovered_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(batch_id, pin_id) DO UPDATE SET
			hearts_snapshot = CASE WHEN excluded.hearts_snapshot > batch_pins.hearts_snapshot THEN excluded.hearts_snapshot ELSE batch_pins.hearts_snapshot END,
			included = CASE WHEN excluded.included = 1 OR batch_pins.included = 1 THEN 1 ELSE 0 END
	`, batchID, pinID, hearts, boolToInt(included), nowISO())
	if err != nil {
		return false, fmt.Errorf("upsert batch pin: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("commit batch pin tx: %w", err)
	}
	return included, nil
}

func (s *Store) ListBatchPins(ctx context.Context, batchID int64, mode string, limit int) ([]BatchPin, error) {
	if limit <= 0 || limit > 2000 {
		limit = 500
	}

	query := `
		WITH task_candidates AS (
			SELECT
				t.batch_id,
				t.url,
				CASE WHEN t.depth <= 0 THEN 1 ELSE t.depth END AS relation_level,
				t.id AS task_id
			FROM crawl_tasks t
			UNION ALL
			SELECT
				ta.batch_id,
				ta.url,
				CASE WHEN ta.depth <= 0 THEN 1 ELSE ta.depth END AS relation_level,
				ta.task_id AS task_id
			FROM crawl_tasks_archive ta
		),
		first_task AS (
			SELECT batch_id, url, relation_level, task_id
			FROM (
				SELECT
					batch_id,
					url,
					relation_level,
					task_id,
					ROW_NUMBER() OVER (
						PARTITION BY batch_id, url
						ORDER BY relation_level ASC, task_id ASC
					) AS rn
				FROM task_candidates
			) ranked
			WHERE rn = 1
		)
		SELECT
			p.id,
			p.url,
			p.hearts,
			COALESCE(first_task.relation_level, 999999) AS relation_level,
			COALESCE(p.image_url, ''),
			COALESCE(p.image_path, ''),
			p.downloaded,
			COALESCE(p.title, ''),
			COALESCE(p.last_checked_at, ''),
			bp.included
		FROM batch_pins bp
		JOIN pins p ON p.id = bp.pin_id
		JOIN batches b ON b.id = bp.batch_id
		LEFT JOIN first_task ON first_task.batch_id = bp.batch_id AND first_task.url = p.url
		WHERE bp.batch_id = ?
	`
	args := []any{batchID}
	if strings.EqualFold(mode, "included") {
		query += ` AND bp.included = 1`
	}
	query += ` ORDER BY relation_level ASC, COALESCE(first_task.task_id, 9223372036854775807) ASC, p.hearts DESC, bp.included DESC, p.updated_at DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list batch pins: %w", err)
	}
	defer rows.Close()

	items := make([]BatchPin, 0)
	for rows.Next() {
		var item BatchPin
		var downloaded, included int
		if err := rows.Scan(
			&item.PinID,
			&item.URL,
			&item.Hearts,
			&item.RelationLevel,
			&item.ImageURL,
			&item.ImagePath,
			&downloaded,
			&item.Title,
			&item.LastCheckedAt,
			&included,
		); err != nil {
			return nil, fmt.Errorf("scan batch pin: %w", err)
		}
		item.Downloaded = intToBool(downloaded)
		item.Included = intToBool(included)
		if item.Downloaded {
			item.ImageViewURL = toWebImageURL(item.ImagePath)
		} else {
			item.ImageViewURL = item.ImageURL
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return items, nil
}

func (s *Store) RuntimeStats(ctx context.Context) (RuntimeStats, error) {
	stats := RuntimeStats{}
	row := s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(pin_agg.total_pins, 0),
			COALESCE(pin_agg.downloaded_pins, 0),
			COALESCE(task_agg.pending_tasks, 0),
			COALESCE(task_agg.processing_tasks, 0),
			COALESCE(task_agg.error_tasks, 0),
			COALESCE(task_agg.done_tasks, 0) + COALESCE(task_archive_agg.done_tasks, 0),
			COALESCE(batch_agg.running_batches, 0)
		FROM
			(
				SELECT
					COUNT(1) AS total_pins,
					SUM(CASE WHEN downloaded = 1 THEN 1 ELSE 0 END) AS downloaded_pins
				FROM pins
			) AS pin_agg
			CROSS JOIN (
				SELECT
					SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending_tasks,
					SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing_tasks,
					SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS error_tasks,
					SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done_tasks
				FROM crawl_tasks
			) AS task_agg
			CROSS JOIN (
				SELECT COUNT(1) AS done_tasks
				FROM crawl_tasks_archive
			) AS task_archive_agg
			CROSS JOIN (
				SELECT COUNT(1) AS running_batches
				FROM batches
				WHERE is_running = 1
			) AS batch_agg
	`)
	if err := row.Scan(
		&stats.TotalPins,
		&stats.DownloadedPins,
		&stats.PendingTasks,
		&stats.DoingTasks,
		&stats.ErrorTasks,
		&stats.DoneTasks,
		&stats.RunningBatches,
	); err != nil {
		return RuntimeStats{}, fmt.Errorf("runtime stats: %w", err)
	}
	return stats, nil
}

func (s *Store) execWithRetry(ctx context.Context, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := withSQLiteBusyRetry(ctx, func() error {
		var err error
		result, err = s.db.ExecContext(ctx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) beginTxWithRetry(ctx context.Context, options *sql.TxOptions) (*sql.Tx, error) {
	var tx *sql.Tx
	err := withSQLiteBusyRetry(ctx, func() error {
		var err error
		tx, err = s.db.BeginTx(ctx, options)
		return err
	})
	if err != nil {
		return nil, err
	}
	return tx, nil
}

func (s *Store) execTxWithRetry(ctx context.Context, tx *sql.Tx, query string, args ...any) (sql.Result, error) {
	var result sql.Result
	err := withSQLiteBusyRetry(ctx, func() error {
		var err error
		result, err = tx.ExecContext(ctx, query, args...)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

func withSQLiteBusyRetry(ctx context.Context, operation func() error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	wait := 40 * time.Millisecond
	for attempt := 0; attempt <= sqliteBusyRetryMax; attempt++ {
		err := operation()
		if err == nil {
			return nil
		}
		if !isSQLiteBusyError(err) || attempt == sqliteBusyRetryMax {
			return err
		}
		if sleepErr := sleepWithContext(ctx, wait); sleepErr != nil {
			return sleepErr
		}
		if wait < 480*time.Millisecond {
			wait *= 2
			if wait > 480*time.Millisecond {
				wait = 480 * time.Millisecond
			}
		}
	}
	return nil
}

func sleepWithContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isSQLiteBusyError(err error) bool {
	if err == nil {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "sqlite_busy") ||
		strings.Contains(lower, "database is locked") ||
		strings.Contains(lower, "database table is locked")
}

func timeKey() string {
	return strings.ReplaceAll(strings.ReplaceAll(nowISO(), ":", ""), "-", "")
}

func clampBatchMaxImages(v int) int {
	if v < 1 {
		return 1
	}
	if v > maxBatchMaxImages {
		return maxBatchMaxImages
	}
	return v
}

func clampBatchMaxDepth(v int) int {
	if v < 1 {
		return 1
	}
	if v > maxBatchMaxDepth {
		return maxBatchMaxDepth
	}
	return v
}

func clampTaskDepth(v int) int {
	if v < 0 {
		return 0
	}
	if v > 9999 {
		return 9999
	}
	return v
}
