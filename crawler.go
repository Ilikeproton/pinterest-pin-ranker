package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/proxy"
)

var (
	absoluteBoardRegex = regexp.MustCompile(`https://www\.pinterest\.[a-z.]+/[^/?"#]+/[^/?"#]+/?`)
	reactionHTMLRegex  = regexp.MustCompile(`data-test-id=["']reactions?-count["'][^>]*>([^<]+)<`)
	reactionJSONRegex  = regexp.MustCompile(`"reactions?_count"\s*:\s*(\d+)`)
	totalReactionRegex = regexp.MustCompile(`"totalReactionCount"\s*:\s*(\d+)`)
	reactionItemRegex  = regexp.MustCompile(`"reactionCount"\s*:\s*(\d+)`)
	pinImageRegex      = regexp.MustCompile(`https:\\/\\/i\.pinimg\.com\\/[^"'\s<>]+`)
	httpsPinImageRegex = regexp.MustCompile(`https://i\.pinimg\.com/[^"'\s<>]+`)
)

const maxDiscoveredLinks = 250
const maxBoardPagesPerPin = 2
const relatedPinsGraphQLQueryHash = "683b9906f7e529faa5fd906fac537aae0011b1a211582abd3309256c266d1ca8"
const relatedPinsGraphQLQueryName = "UnauthCloseupRelatedPinsFeedQuery"
const relatedPinsGraphQLCount = 12

type CrawlResult struct {
	URL      string
	Hearts   int
	ImageURL string
	Title    string
	Links    []string
}

type namedHTTPClient struct {
	name   string
	client *http.Client
}

type Crawler struct {
	imageDir string
	mu       sync.RWMutex
	clients  []namedHTTPClient
}

func NewCrawler(imageDir string) *Crawler {
	clients, err := buildNamedHTTPClients(ProxyConfig{})
	if err != nil {
		clients = []namedHTTPClient{
			{
				name:   "direct",
				client: newHTTPClient(buildTransport(nil, nil)),
			},
		}
	}

	return &Crawler{imageDir: imageDir, clients: clients}
}

func normalizeProxyConfig(config ProxyConfig) (ProxyConfig, error) {
	normalized := ProxyConfig{
		Type:     strings.ToLower(strings.TrimSpace(config.Type)),
		Host:     strings.TrimSpace(config.Host),
		Port:     config.Port,
		Username: strings.TrimSpace(config.Username),
		Password: strings.TrimSpace(config.Password),
	}

	if normalized.Host == "" && normalized.Port <= 0 {
		return ProxyConfig{}, nil
	}
	if normalized.Host == "" {
		return ProxyConfig{}, fmt.Errorf("proxy host is required")
	}
	if normalized.Port <= 0 || normalized.Port > 65535 {
		return ProxyConfig{}, fmt.Errorf("proxy port must be between 1 and 65535")
	}

	switch normalized.Type {
	case "socks", "socks5", "socks5h":
		normalized.Type = "socks5"
	case "http", "https":
		normalized.Type = "http"
	case "", "direct", "local":
		return ProxyConfig{}, nil
	default:
		return ProxyConfig{}, fmt.Errorf("proxy type must be socks5 or http")
	}

	return normalized, nil
}

func buildNamedHTTPClients(config ProxyConfig) ([]namedHTTPClient, error) {
	normalized, err := normalizeProxyConfig(config)
	if err != nil {
		return nil, err
	}
	if normalized.Type == "" {
		return []namedHTTPClient{
			{
				name:   "direct",
				client: newHTTPClient(buildTransport(nil, nil)),
			},
		}, nil
	}

	address := net.JoinHostPort(normalized.Host, fmt.Sprintf("%d", normalized.Port))
	switch normalized.Type {
	case "socks5":
		client, err := newSOCKSClient("socks5://"+address, address, normalized.Username, normalized.Password)
		if err != nil {
			return nil, err
		}
		return []namedHTTPClient{client}, nil
	case "http":
		client, err := newHTTPProxyClient(normalized)
		if err != nil {
			return nil, err
		}
		return []namedHTTPClient{client}, nil
	default:
		return nil, fmt.Errorf("unsupported proxy type %q", normalized.Type)
	}
}

func buildTransport(proxyFunc func(*http.Request) (*url.URL, error), dialContext func(context.Context, string, string) (net.Conn, error)) *http.Transport {
	transport := &http.Transport{
		Proxy:                 proxyFunc,
		MaxIdleConns:          50,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 15 * time.Second,
		ExpectContinueTimeout: 2 * time.Second,
	}
	if dialContext != nil {
		transport.Proxy = nil
		transport.DialContext = dialContext
	}
	return transport
}

func newHTTPClient(transport *http.Transport) *http.Client {
	jar, _ := cookiejar.New(nil)
	return &http.Client{
		Timeout:   25 * time.Second,
		Transport: transport,
		Jar:       jar,
	}
}

func newSOCKSClient(name, proxyAddress, username, password string) (namedHTTPClient, error) {
	var auth *proxy.Auth
	if username != "" || password != "" {
		auth = &proxy.Auth{
			User:     username,
			Password: password,
		}
	}

	dialer, err := proxy.SOCKS5("tcp", proxyAddress, auth, proxy.Direct)
	if err != nil {
		return namedHTTPClient{}, err
	}

	contextDialer, ok := dialer.(proxy.ContextDialer)
	if !ok {
		return namedHTTPClient{}, fmt.Errorf("proxy %s does not support context dial", proxyAddress)
	}

	return namedHTTPClient{
		name:   name,
		client: newHTTPClient(buildTransport(nil, contextDialer.DialContext)),
	}, nil
}

func newHTTPProxyClient(config ProxyConfig) (namedHTTPClient, error) {
	proxyURL := &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(config.Host, fmt.Sprintf("%d", config.Port)),
	}
	if config.Username != "" || config.Password != "" {
		proxyURL.User = url.UserPassword(config.Username, config.Password)
	}
	return namedHTTPClient{
		name:   proxyURL.String(),
		client: newHTTPClient(buildTransport(http.ProxyURL(proxyURL), nil)),
	}, nil
}

func (c *Crawler) snapshotClients() []namedHTTPClient {
	c.mu.RLock()
	defer c.mu.RUnlock()

	clients := make([]namedHTTPClient, len(c.clients))
	copy(clients, c.clients)
	return clients
}

func (c *Crawler) UpdateProxyConfig(config ProxyConfig) error {
	clients, err := buildNamedHTTPClients(config)
	if err != nil {
		return err
	}

	c.mu.Lock()
	oldClients := c.clients
	c.clients = clients
	c.mu.Unlock()

	for _, candidate := range oldClients {
		if candidate.client == nil {
			continue
		}
		if transport, ok := candidate.client.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}

	return nil
}

func (c *Crawler) CrawlPin(ctx context.Context, rawURL string) (CrawlResult, error) {
	normalized, err := normalizePinURL(rawURL)
	if err != nil {
		return CrawlResult{}, err
	}

	htmlRaw, err := c.fetchPage(ctx, normalized)
	if err != nil {
		return CrawlResult{}, err
	}

	doc, err := goquery.NewDocumentFromReader(bytes.NewReader([]byte(htmlRaw)))
	if err != nil {
		return CrawlResult{}, fmt.Errorf("parse html: %w", err)
	}

	hearts := extractHearts(doc, htmlRaw)
	imageURL := extractImageURL(doc, htmlRaw)
	title := extractTitle(doc)
	links := extractLinks(doc, htmlRaw)
	relatedLinks, _ := c.fetchRelatedPinLinksGraphQL(ctx, normalized)
	links = mergeDiscoveredLinks(links, relatedLinks, maxDiscoveredLinks)
	links = removeLink(links, normalized)

	return CrawlResult{
		URL:      normalized,
		Hearts:   hearts,
		ImageURL: imageURL,
		Title:    title,
		Links:    links,
	}, nil
}

func (c *Crawler) expandLinksFromBoardPages(ctx context.Context, links []string, boardURLs []string) []string {
	seen := make(map[string]struct{}, len(links)+32)
	result := make([]string, 0, len(links)+32)
	for _, link := range links {
		normalized, err := normalizePinURL(link)
		if err != nil {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}

	boardCount := 0
	for _, boardURL := range boardURLs {
		if boardCount >= maxBoardPagesPerPin || len(result) >= maxDiscoveredLinks {
			break
		}
		boardCount++

		htmlRaw, err := c.fetchPage(ctx, boardURL)
		if err != nil {
			continue
		}
		doc, err := goquery.NewDocumentFromReader(bytes.NewReader([]byte(htmlRaw)))
		if err != nil {
			continue
		}

		for _, link := range extractLinks(doc, htmlRaw) {
			if len(result) >= maxDiscoveredLinks {
				break
			}
			normalized, err := normalizePinURL(link)
			if err != nil {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			result = append(result, normalized)
		}
	}

	sort.Strings(result)
	return result
}

func (c *Crawler) fetchPage(ctx context.Context, rawURL string) (string, error) {
	headers := map[string]string{
		"User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
		"Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"Accept-Language": "en-US,en;q=0.9",
	}

	body, err := c.fetchWithFallback(ctx, rawURL, headers, 12*1024*1024)
	if err != nil {
		return "", fmt.Errorf("request pin page: %w", err)
	}
	return string(body), nil
}

func (c *Crawler) fetchRelatedPinLinksGraphQL(ctx context.Context, pinURL string) ([]string, error) {
	pinID := pinIDFromURL(pinURL)
	if pinID == "" {
		return nil, fmt.Errorf("invalid pin id")
	}

	target, err := url.Parse(pinURL)
	if err != nil {
		return nil, err
	}
	queryURL := fmt.Sprintf("%s://%s/_/graphql/", target.Scheme, target.Host)

	requestBody := map[string]any{
		"queryHash": relatedPinsGraphQLQueryHash,
		"variables": map[string]any{
			"pinId":         pinID,
			"count":         relatedPinsGraphQLCount,
			"isBot":         false,
			"isDesktop":     true,
			"isAuth":        false,
			"contextPinIds": []string{pinID},
		},
	}
	bodyData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, err
	}

	var lastErr error
	for _, candidate := range c.snapshotClients() {
		csrf := csrfTokenFromCookieJar(candidate.client, pinURL)
		if csrf == "" {
			if err := bootstrapPinterestCookies(ctx, candidate.client, pinURL); err != nil {
				lastErr = err
				continue
			}
			csrf = csrfTokenFromCookieJar(candidate.client, pinURL)
		}
		headers := map[string]string{
			"User-Agent":               "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
			"Accept":                   "application/json",
			"Accept-Language":          "en-US,en;q=0.9",
			"Content-Type":             "application/json",
			"X-Requested-With":         "XMLHttpRequest",
			"X-Pinterest-Source-Url":   pinURL,
			"X-Pinterest-GraphQL-Name": relatedPinsGraphQLQueryName,
			"X-Pinterest-AppState":     "active",
			"Referer":                  pinURL,
		}
		if csrf != "" {
			headers["X-CSRFToken"] = csrf
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, queryURL, bytes.NewReader(bodyData))
		if err != nil {
			lastErr = err
			continue
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}

		resp, err := candidate.client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		payload, readErr := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}
		if resp.StatusCode >= 400 {
			lastErr = fmt.Errorf("%s: status %d", candidate.name, resp.StatusCode)
			continue
		}

		links := parseRelatedPinLinksFromGraphQL(payload)
		if len(links) > 0 {
			return links, nil
		}
	}

	if lastErr != nil {
		return nil, lastErr
	}
	return nil, nil
}

func bootstrapPinterestCookies(ctx context.Context, client *http.Client, pageURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 512*1024))
	_ = resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("bootstrap status %d", resp.StatusCode)
	}
	return nil
}

func csrfTokenFromCookieJar(client *http.Client, pageURL string) string {
	if client == nil || client.Jar == nil {
		return ""
	}
	parsed, err := url.Parse(pageURL)
	if err != nil {
		return ""
	}
	for _, cookie := range client.Jar.Cookies(parsed) {
		if strings.EqualFold(cookie.Name, "csrftoken") {
			return strings.TrimSpace(cookie.Value)
		}
	}
	return ""
}

func parseRelatedPinLinksFromGraphQL(payload []byte) []string {
	type edgeNode struct {
		Typename string `json:"__typename"`
		EntityID string `json:"entityId"`
	}
	type relatedResponse struct {
		Data struct {
			Related struct {
				Data struct {
					Connection struct {
						Edges []struct {
							Node edgeNode `json:"node"`
						} `json:"edges"`
					} `json:"connection"`
				} `json:"data"`
			} `json:"v3RelatedPinsForPinSeoQuery"`
		} `json:"data"`
	}

	result := relatedResponse{}
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil
	}

	seen := make(map[string]struct{}, 64)
	links := make([]string, 0, 64)
	for _, edge := range result.Data.Related.Data.Connection.Edges {
		if edge.Node.Typename != "Pin" {
			continue
		}
		id := strings.TrimSpace(edge.Node.EntityID)
		if id == "" {
			continue
		}
		link := fmt.Sprintf("https://www.pinterest.com/pin/%s/", id)
		if _, ok := seen[link]; ok {
			continue
		}
		seen[link] = struct{}{}
		links = append(links, link)
		if len(links) >= maxDiscoveredLinks {
			break
		}
	}
	return links
}

func mergeDiscoveredLinks(primary []string, incoming []string, limit int) []string {
	if limit <= 0 {
		limit = maxDiscoveredLinks
	}
	if len(incoming) == 0 {
		if len(primary) > limit {
			return primary[:limit]
		}
		return primary
	}

	seen := make(map[string]struct{}, len(primary)+len(incoming))
	out := make([]string, 0, len(primary)+len(incoming))
	add := func(raw string) {
		if len(out) >= limit {
			return
		}
		normalized, err := normalizePinURL(raw)
		if err != nil {
			return
		}
		if _, exists := seen[normalized]; exists {
			return
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}

	for _, item := range primary {
		add(item)
	}
	for _, item := range incoming {
		add(item)
	}

	return out
}

func (c *Crawler) DownloadImage(ctx context.Context, pinURL, imageURL string) (string, error) {
	imageURL = strings.TrimSpace(imageURL)
	if imageURL == "" {
		return "", fmt.Errorf("empty image url")
	}

	pinID := pinIDFromURL(pinURL)
	if pinID == "" {
		pinID = fmt.Sprintf("pin_%d", time.Now().UnixNano())
	}
	ext := chooseImageExt(imageURL)
	fileName := pinID + ext
	targetPath := filepath.Join(c.imageDir, fileName)

	if _, err := os.Stat(targetPath); err == nil {
		return targetPath, nil
	}

	headers := map[string]string{
		"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		"Referer":    "https://www.pinterest.com/",
	}
	payload, err := c.fetchWithFallback(ctx, imageURL, headers, 25*1024*1024)
	if err != nil {
		return "", fmt.Errorf("download image request: %w", err)
	}

	tmp, err := os.CreateTemp(c.imageDir, pinID+"_*.tmp")
	if err != nil {
		return "", fmt.Errorf("create temp image file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}()

	if _, err := tmp.Write(payload); err != nil {
		return "", fmt.Errorf("save image data: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("close temp image: %w", err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		if _, statErr := os.Stat(targetPath); statErr == nil {
			return targetPath, nil
		}
		return "", fmt.Errorf("rename image file: %w", err)
	}

	return targetPath, nil
}

func (c *Crawler) fetchWithFallback(ctx context.Context, rawURL string, headers map[string]string, maxBytes int64) ([]byte, error) {
	if len(c.clients) == 0 {
		return nil, fmt.Errorf("no http clients configured")
	}

	var lastErr error
	for _, candidate := range c.snapshotClients() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		for key, value := range headers {
			req.Header.Set(key, value)
		}

		resp, err := candidate.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("%s: %w", candidate.name, err)
			continue
		}

		body, readErr := io.ReadAll(io.LimitReader(resp.Body, maxBytes))
		_ = resp.Body.Close()
		if readErr != nil {
			lastErr = fmt.Errorf("%s: read response: %w", candidate.name, readErr)
			continue
		}

		if resp.StatusCode >= 400 {
			message := strings.TrimSpace(string(body))
			if len(message) > 300 {
				message = message[:300]
			}
			httpErr := fmt.Errorf("%s: status %d: %s", candidate.name, resp.StatusCode, message)
			if shouldRetryStatus(resp.StatusCode) {
				lastErr = httpErr
				continue
			}
			return nil, httpErr
		}

		return body, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("all request attempts failed")
	}
	return nil, lastErr
}

func shouldRetryStatus(code int) bool {
	return code == http.StatusForbidden || code == http.StatusTooManyRequests || code >= 500
}

func extractHearts(doc *goquery.Document, htmlRaw string) int {
	text := strings.TrimSpace(doc.Find(`[data-test-id='reaction-count'], [data-test-id='reactions-count']`).First().Text())
	if count := parseCount(text); count > 0 {
		return count
	}

	if match := reactionHTMLRegex.FindStringSubmatch(htmlRaw); len(match) > 1 {
		if count := parseCount(match[1]); count > 0 {
			return count
		}
	}

	if match := reactionJSONRegex.FindStringSubmatch(htmlRaw); len(match) > 1 {
		if count := parseCount(match[1]); count > 0 {
			return count
		}
	}
	if match := totalReactionRegex.FindStringSubmatch(htmlRaw); len(match) > 1 {
		if count := parseCount(match[1]); count > 0 {
			return count
		}
	}
	if match := reactionItemRegex.FindStringSubmatch(htmlRaw); len(match) > 1 {
		if count := parseCount(match[1]); count > 0 {
			return count
		}
	}

	return 0
}

func extractImageURL(doc *goquery.Document, htmlRaw string) string {
	selectors := []string{
		`img[elementtiming='closeup-image-main-MainPinImage']`,
		`img[data-test-id='closeup-image']`,
		`meta[property='og:image']`,
	}

	for _, sel := range selectors {
		selection := doc.Find(sel).First()
		if selection.Length() == 0 {
			continue
		}
		if sel == `meta[property='og:image']` {
			if value, ok := selection.Attr("content"); ok && strings.TrimSpace(value) != "" {
				return strings.TrimSpace(value)
			}
			continue
		}
		if value, ok := selection.Attr("src"); ok && strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
		if value, ok := selection.Attr("srcset"); ok {
			parts := strings.Split(value, ",")
			if len(parts) > 0 {
				piece := strings.TrimSpace(parts[len(parts)-1])
				fields := strings.Fields(piece)
				if len(fields) > 0 {
					return fields[0]
				}
			}
		}
	}

	if match := httpsPinImageRegex.FindString(htmlRaw); match != "" {
		return strings.TrimSpace(match)
	}

	if match := pinImageRegex.FindString(htmlRaw); match != "" {
		decoded := strings.ReplaceAll(match, `\/`, "/")
		return strings.TrimSpace(decoded)
	}

	return ""
}

func extractTitle(doc *goquery.Document) string {
	candidates := []string{
		strings.TrimSpace(getAttr(doc, `meta[property='og:title']`, "content")),
		strings.TrimSpace(getAttr(doc, `meta[property='og:description']`, "content")),
		strings.TrimSpace(getAttr(doc, `img[elementtiming='closeup-image-main-MainPinImage']`, "alt")),
		strings.TrimSpace(doc.Find("title").First().Text()),
	}
	for _, c := range candidates {
		if c != "" {
			return c
		}
	}
	return ""
}

func getAttr(doc *goquery.Document, selector, attr string) string {
	if v, ok := doc.Find(selector).First().Attr(attr); ok {
		return v
	}
	return ""
}

func extractLinks(doc *goquery.Document, htmlRaw string) []string {
	_ = htmlRaw

	seen := make(map[string]struct{}, 128)
	result := make([]string, 0, 64)
	add := func(raw string) {
		if len(result) >= maxDiscoveredLinks {
			return
		}
		if strings.Contains(raw, "/pin/create/") {
			return
		}
		normalized, err := normalizePinURL(raw)
		if err != nil {
			return
		}
		if _, exists := seen[normalized]; exists {
			return
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}

	// Prefer visible pin tiles with image payload; these are usually closer to "related" results.
	doc.Find(`a[href*="/pin/"]`).Each(func(_ int, selection *goquery.Selection) {
		href, ok := selection.Attr("href")
		if !ok {
			return
		}
		if selection.Find("img").Length() > 0 {
			add(href)
		}
	})

	// Fallback: if tile links are too few, include all pin anchors from DOM.
	if len(result) < 12 {
		doc.Find(`a[href*="/pin/"]`).Each(func(_ int, selection *goquery.Selection) {
			href, ok := selection.Attr("href")
			if !ok {
				return
			}
			add(href)
		})
	}

	return result
}

func removeLink(links []string, target string) []string {
	if target == "" || len(links) == 0 {
		return links
	}
	filtered := make([]string, 0, len(links))
	for _, item := range links {
		if item == target {
			continue
		}
		filtered = append(filtered, item)
	}
	return filtered
}

func extractBoardURLs(doc *goquery.Document, htmlRaw string) []string {
	seen := make(map[string]struct{}, 16)
	add := func(raw string) {
		if len(seen) >= 20 {
			return
		}
		normalized, err := normalizeBoardURL(raw)
		if err != nil {
			return
		}
		seen[normalized] = struct{}{}
	}

	doc.Find("a[href]").Each(func(_ int, selection *goquery.Selection) {
		href, ok := selection.Attr("href")
		if !ok {
			return
		}
		if !strings.Contains(href, "/pin/") {
			add(href)
		}
	})

	for _, match := range absoluteBoardRegex.FindAllString(htmlRaw, -1) {
		add(match)
	}

	result := make([]string, 0, len(seen))
	for link := range seen {
		result = append(result, link)
	}
	sort.Strings(result)
	return result
}

func normalizeBoardURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("empty board url")
	}

	if strings.HasPrefix(raw, "//") {
		raw = "https:" + raw
	}
	if strings.HasPrefix(raw, "/") {
		raw = "https://www.pinterest.com" + raw
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		raw = "https://" + raw
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	host := strings.ToLower(u.Host)
	if !strings.Contains(host, "pinterest.") {
		return "", fmt.Errorf("not pinterest host")
	}

	segments := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(segments) != 2 {
		return "", fmt.Errorf("not board path")
	}
	if !isBoardSegment(segments[0]) || !isBoardSegment(segments[1]) {
		return "", fmt.Errorf("invalid board path segments")
	}
	if strings.EqualFold(segments[0], "pin") || strings.EqualFold(segments[0], "ideas") {
		return "", fmt.Errorf("not board path")
	}

	return fmt.Sprintf("https://www.pinterest.com/%s/%s/", segments[0], segments[1]), nil
}

func isBoardSegment(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	disallowed := []string{
		"pin", "ideas", "search", "business", "about", "today", "topics",
		"login", "signup", "settings", "_tools", "resource", "source", "_",
	}
	for _, item := range disallowed {
		if strings.EqualFold(raw, item) {
			return false
		}
	}
	return true
}
