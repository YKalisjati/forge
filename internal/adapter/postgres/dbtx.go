package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type txContextKey struct{}

// DBTX encapsulates operations common to both *pgxpool.Pool and pgx.Tx.
type DBTX interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Transactor provides transaction management abstractions.
type Transactor interface {
	WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error
}

// UnitOfWork manages database transactions on a pgxpool.Pool.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork creates a new UnitOfWork.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

// WithinTransaction executes fn within an explicit PostgreSQL transaction boundary.
// If fn returns an error or panics, the transaction is safely rolled back.
func (u *UnitOfWork) WithinTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		_ = tx.Rollback(ctx)
	}()

	txCtx := context.WithValue(ctx, txContextKey{}, tx)
	if err := fn(txCtx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// extractDB retrieves the active pgx.Tx from context, or falls back to pool.
func extractDB(ctx context.Context, defaultPool *pgxpool.Pool) DBTX {
	if ctx != nil {
		if tx, ok := ctx.Value(txContextKey{}).(pgx.Tx); ok && tx != nil {
			return tx
		}
	}
	return defaultPool
}
