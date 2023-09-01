package yqe

import (
	"context"
	"fmt"
	"github.com/flymedllva/ydb-go-qb/yqb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

// Select accepting SQL string and parameters
func (e *Executor) Select(ctx context.Context, dst any, query string, params []table.ParameterOption) (table.Transaction, error) {
	var (
		tx  table.Transaction
		err error
	)
	switch {
	case e.sessionRunner != nil:
		tx, err = ScanDefaultAPI.Select(ctx, e.txc, e.sessionRunner, dst, query, table.NewQueryParameters(
			params...,
		))
		if err != nil {
			return tx, fmt.Errorf("ScanDefaultAPI.Select: %w", err)
		}
	case e.transactionRunner != nil:
		err = ScanDefaultAPI.SelectTx(ctx, e.transactionRunner, dst, query, table.NewQueryParameters(
			params...,
		))
		tx, _ = e.transactionRunner.(table.Transaction)
		if err != nil {
			return tx, fmt.Errorf("ScanDefaultAPI.SelectTx: %w", err)
		}
	default:
		return tx, fmt.Errorf("no runner")
	}

	return tx, nil
}

// SelectX select accepting SQL builder
func (e *Executor) SelectX(ctx context.Context, dst any, query yqb.YdbSqlizer) (table.Transaction, error) {
	ydbSqlStr, ydbParams, err := query.ToYdbSql()
	if err != nil {
		return nil, fmt.Errorf("query.ToYdbSql: %w", err)
	}

	return e.Select(ctx, dst, ydbSqlStr, ydbParams)
}
