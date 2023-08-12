package yqr

import (
	"context"
	"fmt"

	"github.com/flymedllva/ydb-go-qb/yqb"
	"github.com/flymedllva/ydb-go-qb/yscan"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

// SqlizedExecer exec query use qb.YdbSqlizer
type SqlizedExecer interface {
	Exec(ctx context.Context, query yqb.YdbSqlizer) (table.Transaction, result.Result, error)
}

// SqlizedGetter get query use qb.YdbSqlizer
type SqlizedGetter interface {
	Get(ctx context.Context, dst any, query yqb.YdbSqlizer) (table.Transaction, error)
}

// SqlizedSelector select query use qb.YdbSqlizer
type SqlizedSelector interface {
	Select(ctx context.Context, dst any, query yqb.YdbSqlizer) (table.Transaction, error)
}

// SessionRunner run query via session
type SessionRunner struct {
	tx     *table.TransactionControl
	runner yscan.SessionQuerier
}

// ViaSession run query via session
func ViaSession(tx *table.TransactionControl, runner yscan.SessionQuerier) *SessionRunner {
	return &SessionRunner{
		tx:     tx,
		runner: runner,
	}
}

// Exec exec query
func (s *SessionRunner) Exec(ctx context.Context, query yqb.YdbSqlizer) (table.Transaction, result.Result, error) {
	ydbSqlStr, ydbParams, err := query.ToYdbSql()
	if err != nil {
		return nil, nil, err
	}

	trx, r, err := s.runner.Execute(ctx, s.tx, ydbSqlStr, table.NewQueryParameters(
		ydbParams...,
	))
	if err != nil {
		return nil, nil, err
	}

	return trx, r, nil
}

// Get a get query
func (s *SessionRunner) Get(ctx context.Context, dst any, query yqb.YdbSqlizer) (table.Transaction, error) {
	ydbSqlStr, ydbParams, err := query.ToYdbSql()
	if err != nil {
		return nil, err
	}

	trx, err := ScanDefaultAPI.Get(ctx, s.tx, s.runner, dst, ydbSqlStr, table.NewQueryParameters(
		ydbParams...,
	))
	if err != nil {
		return nil, err
	}

	return trx, nil
}

// Select select query
func (s *SessionRunner) Select(ctx context.Context, dst any, query yqb.YdbSqlizer) (table.Transaction, error) {
	ydbSqlStr, ydbParams, err := query.ToYdbSql()
	if err != nil {
		return nil, err
	}

	trx, err := ScanDefaultAPI.Select(ctx, s.tx, s.runner, dst, ydbSqlStr, table.NewQueryParameters(
		ydbParams...,
	))
	if err != nil {
		return nil, fmt.Errorf("Select: %w", err)
	}

	return trx, nil
}
