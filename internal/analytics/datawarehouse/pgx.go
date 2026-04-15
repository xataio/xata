package datawarehouse

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

type PGXConfig struct {
	PostgresURL    string
	AdvisoryLockID *int64
	Schema         *SchemaInitializer
}

type pgxDW struct {
	pool   *pgxpool.Pool
	conn   *pgxpool.Conn
	lockID *int64
	schema *SchemaInitializer
}

func NewPGX(ctx context.Context, config PGXConfig) (DW, error) {
	pool, err := pgxpool.New(ctx, config.PostgresURL)
	if err != nil {
		return nil, fmt.Errorf("connect warehouse: %w", err)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("acquire connection: %w", err)
	}

	if config.AdvisoryLockID != nil {
		if _, err := conn.Exec(ctx, "select pg_advisory_lock($1)", *config.AdvisoryLockID); err != nil {
			conn.Release()
			pool.Close()
			return nil, fmt.Errorf("acquire advisory lock: %w", err)
		}
	}

	return &pgxDW{
		pool:   pool,
		conn:   conn,
		lockID: config.AdvisoryLockID,
		schema: config.Schema,
	}, nil
}

func (d *pgxDW) EnsureSchema(ctx context.Context) error {
	if d.schema == nil {
		return nil
	}
	return d.schema.EnsureSchema(ctx, d.conn)
}

func (d *pgxDW) RunInTx(ctx context.Context, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (d *pgxDW) Close() {
	if d.lockID != nil {
		var unlocked bool
		if err := d.conn.QueryRow(context.Background(), "select pg_advisory_unlock($1)", *d.lockID).Scan(&unlocked); err != nil {
			log.Error().Err(err).Msg("release advisory lock")
		} else if !unlocked {
			log.Warn().Int64("lock_id", *d.lockID).Msg("advisory lock not held at close time")
		}
	}

	d.conn.Release()
	d.pool.Close()
}
