package migrator

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	// Register pgx driver with database/sql for migrations.
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/YKalisjati/forge/migrations"
	"github.com/YKalisjati/forge/pkg/config"
)

// Migrator manages database schema migrations.
type Migrator struct {
	m   *migrate.Migrate
	log *slog.Logger
}

// New creates a new Migrator with embedded migration scripts.
func New(cfg *config.PostgresConfig, log *slog.Logger) (*Migrator, error) {
	d, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("failed to load embedded migrations: %w", err)
	}

	db, err := sql.Open("pgx", cfg.DSN())
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	driver, err := postgres.WithInstance(db, &postgres.Config{
		MigrationsTable: "schema_migrations",
	})
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to initialize postgres migration driver: %w", err)
	}

	m, err := migrate.NewWithInstance("iofs", d, "postgres", driver)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("failed to create migrate instance: %w", err)
	}

	return &Migrator{
		m:   m,
		log: log,
	}, nil
}

// Up applies all pending migrations.
func (mig *Migrator) Up() error {
	mig.log.Info("running pending database migrations (up)")
	err := mig.m.Up()
	if err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			mig.log.Info("database schema is already up to date")
			return nil
		}
		return fmt.Errorf("failed to execute migrations up: %w", err)
	}
	mig.log.Info("migrations completed successfully")
	return nil
}

// Down rolls back the last applied migration step.
func (mig *Migrator) Down() error {
	mig.log.Info("rolling back database migration by 1 step (down)")
	err := mig.m.Steps(-1)
	if err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			mig.log.Info("no migrations available to roll back")
			return nil
		}
		return fmt.Errorf("failed to execute migration down: %w", err)
	}
	mig.log.Info("migration roll back completed")
	return nil
}

// Version returns the current migration version and dirty status.
func (mig *Migrator) Version() (uint, bool, error) {
	v, dirty, err := mig.m.Version()
	if err != nil {
		if errors.Is(err, migrate.ErrNilVersion) {
			return 0, false, nil
		}
		return 0, false, err
	}
	return v, dirty, nil
}

// Force sets the database migration version to clean dirty states.
func (mig *Migrator) Force(v int) error {
	mig.log.Warn("forcing migration version", slog.Int("version", v))
	return mig.m.Force(v)
}

// Close gracefully closes the migrator connections.
func (mig *Migrator) Close() error {
	srcErr, dbErr := mig.m.Close()
	if srcErr != nil {
		return srcErr
	}
	return dbErr
}
