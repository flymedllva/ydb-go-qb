package yqr

import (
	"context"
	"fmt"

	"github.com/flymedllva/ydb-go-qb/yqb"
	"github.com/flymedllva/ydb-go-qb/yscan"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

// SqlizedExecerTx exec query use qb.YdbSqlizer
type SqlizedExecerTx interface {
	Exec(ctx context.Context, query yqb.YdbSqlizer) (result.Result, error)
}
type SqlizedGetterTx interface {
	Get(ctx context.Context, dst any, query yqb.YdbSqlizer) error
}
type SqlizedSelectorTx interface {
	Select(ctx context.Context, dst any, query yqb.YdbSqlizer) error
}

// TransactionRunner run query via transaction
type TransactionRunner struct {
	runner yscan.TransactionQuerier
}

// ViaTransaction run query via transaction
func ViaTransaction(runner yscan.TransactionQuerier) *TransactionRunner {
	return &TransactionRunner{
		runner: runner,
	}
}

// Exec exec query
func (s *TransactionRunner) Exec(ctx context.Context, query yqb.YdbSqlizer) (result.Result, error) {
	ydbSqlStr, ydbParams, err := query.ToYdbSql()
	if err != nil {
		return nil, err
	}

	r, err := s.runner.Execute(ctx, ydbSqlStr, table.NewQueryParameters(
		ydbParams...,
	))
	if err != nil {
		return nil, fmt.Errorf("Exec: %w", err)
	}

	return r, nil
}

// Get a get query
func (s *TransactionRunner) Get(ctx context.Context, dst any, query yqb.YdbSqlizer) error {
	ydbSqlStr, ydbParams, err := query.ToYdbSql()
	if err != nil {
		return err
	}

	err = ScanDefaultAPI.GetTx(ctx, s.runner, dst, ydbSqlStr, table.NewQueryParameters(
		ydbParams...,
	))
	if err != nil {
		return fmt.Errorf("GetTx: %w", err)
	}

	return nil
}

// Select select query
func (s *TransactionRunner) Select(ctx context.Context, dst any, query yqb.YdbSqlizer) error {
	ydbSqlStr, ydbParams, err := query.ToYdbSql()
	if err != nil {
		return err
	}

	err = ScanDefaultAPI.SelectTx(ctx, s.runner, dst, ydbSqlStr, table.NewQueryParameters(
		ydbParams...,
	))
	if err != nil {
		return fmt.Errorf("SelectTx: %w", err)
	}

	return nil
}
