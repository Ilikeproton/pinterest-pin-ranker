package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

type runConfig struct {
	mode    string
	addr    string
	dataDir string
	debug   bool
}

func main() {
	cfg, err := parseRunConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		printUsageAndExit(1)
	}

	app, err := newPinPulseApp(cfg.dataDir)
	if err != nil {
		log.Fatalf("start app: %v", err)
	}
	defer func() {
		if closeErr := app.Close(); closeErr != nil {
			log.Printf("shutdown warning: %v", closeErr)
		}
	}()

	switch cfg.mode {
	case "server":
		log.Printf("PinPulse server mode on http://%s", app.serverAddress(cfg.addr))
		if err := app.RunServer(cfg.addr); err != nil {
			log.Fatalf("server exit: %v", err)
		}
	default:
		if err := app.RunDesktop(cfg.debug); err != nil {
			log.Fatalf("desktop exit: %v", err)
		}
	}
}

func parseRunConfig(args []string) (runConfig, error) {
	cfg := runConfig{
		mode: "desktop",
		addr: "127.0.0.1:0",
	}

	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cfg.mode = strings.ToLower(strings.TrimSpace(args[0]))
		args = args[1:]
	}
	if cfg.mode == "" {
		cfg.mode = "desktop"
	}
	if cfg.mode != "desktop" && cfg.mode != "server" {
		return cfg, fmt.Errorf("unknown mode %q", cfg.mode)
	}

	if cfg.mode == "server" {
		port := strings.TrimSpace(os.Getenv("PORT"))
		if port == "" {
			port = "8401"
		}
		cfg.addr = "127.0.0.1:" + port
	}

	flags := flag.NewFlagSet("pinpulse", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.StringVar(&cfg.addr, "addr", cfg.addr, "listen address")
	flags.StringVar(&cfg.dataDir, "data-dir", "", "override data directory")
	flags.BoolVar(&cfg.debug, "debug", false, "enable desktop dev tools")
	if err := flags.Parse(args); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func printUsageAndExit(code int) {
	fmt.Fprintln(os.Stderr, "usage:")
	fmt.Fprintln(os.Stderr, "  pinpulse.exe [desktop] [--data-dir <dir>] [--debug]")
	fmt.Fprintln(os.Stderr, "  pinpulse.exe server [--addr 127.0.0.1:8401] [--data-dir <dir>]")
	os.Exit(code)
}
