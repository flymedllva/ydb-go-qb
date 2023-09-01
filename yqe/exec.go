package yqe

import (
	"context"
	"fmt"

	"github.com/flymedllva/ydb-go-qb/yqb"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

// Exec accepting SQL string and parameters
func (e *Executor) Exec(ctx context.Context, query string, params []table.ParameterOption) (table.Transaction, result.Result, error) {
	var (
		tx  table.Transaction
		res result.Result
		err error
	)
	switch {
	case e.sessionRunner != nil:
		tx, res, err = e.sessionRunner.Execute(ctx, e.txc, query, table.NewQueryParameters(
			params...,
		))
		if err != nil {
			return tx, res, fmt.Errorf("sessionRunner.Execute: %w", err)
		}
	case e.transactionRunner != nil:
		res, err = e.transactionRunner.Execute(ctx, query, table.NewQueryParameters(
			params...,
		))
		tx, _ = e.transactionRunner.(table.Transaction)
		if err != nil {
			return tx, res, fmt.Errorf("transactionRunner.Execute: %w", err)
		}
	default:
		return tx, res, fmt.Errorf("no runner")
	}

	return tx, res, nil
}

// ExecX select accepting SQL builder
func (e *Executor) ExecX(ctx context.Context, query yqb.YdbSqlizer) (table.Transaction, result.Result, error) {
	ydbSqlStr, ydbParams, err := query.ToYdbSql()
	if err != nil {
		return nil, nil, fmt.Errorf("query.ToYdbSql: %w", err)
	}

	return e.Exec(ctx, ydbSqlStr, ydbParams)
}
