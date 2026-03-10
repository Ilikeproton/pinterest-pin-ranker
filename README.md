# PinPulse

English | [简体中文](./README.zh-CN.md)

PinPulse is a desktop-first Pinterest Pin crawler and local image workspace. It starts from a single Pinterest link, discovers related content layer by layer, counts hearts, and stores qualified images in a local database and image folder.

## Quick Links

- [Chinese README](./README.zh-CN.md)
- [License](./LICENSE)
- [Windows Build Script](./build-windows.bat)

## Preview

![PinPulse Preview](./screen.png)

## Core Features

- Windows desktop app that can be packaged as a single `exe`
- Batch-based crawling with configurable `seed_url`, `threshold`, `max_images`, and `max_depth`
- Local SQLite storage with a local image library
- Built-in proxy settings with `Direct`, `SOCKS5`, and `HTTP`
- Multi-language UI with English as the default language and a top-right language switcher
- Automatic batch compaction to reduce useless link buildup

## Quick Start

### Run in Development

```bash
go run .
```

### Build for Windows

```bat
build-windows.bat
```

The packaged executable is written to:

```text
bin\PinPulse.exe
```

## Usage Guide

### 1. Create a batch

In the `Create Batch` panel on the dashboard, fill in:

- `Batch Name`: a readable name for the task
- `Seed Link`: a Pinterest Pin URL
- `Heart Threshold`: heart threshold, default `2`
- `Max Saved Images`: maximum number of saved images; do not set it much higher than `100`
- `Max Depth`: crawl depth; `2` or `3` is usually enough

### 2. Recommended settings

- `Heart Threshold = 2`
  Images with at least 2 hearts can enter the saved result set
- `Max Saved Images = 100`
  Larger values increase the chance of unrelated images appearing later
- `Max Depth = 2`
  Relevance usually drops noticeably after depth 2

### 3. Start crawling

After creating the batch:

1. Open the target batch
2. Click `Start Batch`
3. If you want the global scheduler to run, click `Start All` on the dashboard

### 4. Review results

In the batch detail page:

- `View Original Link`: opens the original Pinterest page
- `Locate`: if the image already exists locally, it opens Explorer and selects the file; otherwise it opens the image source URL
- Top-right gear button: opens proxy settings
- Top-right language selector: switches the UI language

## Data Storage

Default data locations:

- Development: `./data/`
- Windows packaged build: `data/` next to `bin\PinPulse.exe`
- Override with `PINPULSE_DATA_DIR` or `--data-dir`

Main files:

- Database: `data/app.db`
- Image folder: `data/images/`
- Frontend source: `web/`

## Network and Proxy

PinPulse supports:

- Direct connection
- SOCKS5 proxy
- HTTP proxy

Proxy settings are available from the gear button in the top-right corner. If you do not configure a proxy, PinPulse uses the local direct connection.

## Pages and Endpoints

- `/`: dashboard
- `/batch/{id}`: batch detail page
- `/api/health`: startup health check

## Tech Stack

- Go
- SQLite
- Vanilla JavaScript

## Notes

- This project is not affiliated with Pinterest
- Make sure your usage follows target site terms, rate limits, copyright rules, and local law
- This tool is suitable for local research, asset organization, and automation workflows; do not use it for risky or non-compliant scenarios

## Need More Custom Software?

If you need custom software, automation workflows, AI tools, or a desktop/data collection tool built around your business process, use the contact information below.

## Contact Email

<img src="email.png" width="270">
