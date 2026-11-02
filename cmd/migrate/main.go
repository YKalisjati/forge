package main

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/YKalisjati/forge/pkg/config"
	"github.com/YKalisjati/forge/pkg/logger"
	"github.com/YKalisjati/forge/pkg/migrator"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load configuration: %v\n", err)
		os.Exit(1)
	}

	log := logger.New("forge-migrator", &cfg.App)

	mig, err := migrator.New(&cfg.Postgres, log)
	if err != nil {
		log.Error("failed to initialize migrator", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer func() {
		_ = mig.Close()
	}()

	action := "up"
	if len(os.Args) > 1 {
		action = os.Args[1]
	}

	switch action {
	case "up":
		if err := mig.Up(); err != nil {
			log.Error("migration up failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
	case "down":
		if err := mig.Down(); err != nil {
			log.Error("migration down failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
	case "version":
		v, dirty, err := mig.Version()
		if err != nil {
			log.Error("failed to get migration version", slog.String("error", err.Error()))
			os.Exit(1)
		}
		log.Info("current schema version", slog.Uint64("version", uint64(v)), slog.Bool("dirty", dirty))
	case "force":
		if len(os.Args) < 3 {
			log.Error("force command requires version argument: migrate force <version>")
			os.Exit(1)
		}
		targetVersion, err := strconv.Atoi(os.Args[2])
		if err != nil {
			log.Error("invalid version argument", slog.String("error", err.Error()))
			os.Exit(1)
		}
		if err := mig.Force(targetVersion); err != nil {
			log.Error("force migration failed", slog.String("error", err.Error()))
			os.Exit(1)
		}
	default:
		log.Error("unknown migration action", slog.String("action", action))
		fmt.Println("Usage: migrate [up|down|version|force <version>]")
		os.Exit(1)
	}
}
