package yqe

import (
	"context"

	"github.com/flymedllva/ydb-go-qb/yqb"
	"github.com/flymedllva/ydb-go-qb/yscan"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

// SqlizedExecer exec query use qb.YdbSqlizer
type SqlizedExecer interface {
	Exec(ctx context.Context, query string, params []table.ParameterOption) (table.Transaction, result.Result, error)
	ExecX(ctx context.Context, query yqb.YdbSqlizer) (table.Transaction, result.Result, error)
}

// SqlizedGetter get query use qb.YdbSqlizer
type SqlizedGetter interface {
	Get(ctx context.Context, dst any, query string, params []table.ParameterOption) (table.Transaction, error)
	GetX(ctx context.Context, dst any, query yqb.YdbSqlizer) (table.Transaction, error)
}

// SqlizedSelector select query use qb.YdbSqlizer
type SqlizedSelector interface {
	Select(ctx context.Context, dst any, query string, params []table.ParameterOption) (table.Transaction, error)
	SelectX(ctx context.Context, dst any, query yqb.YdbSqlizer) (table.Transaction, error)
}

// Executor runner query
type Executor struct {
	txc               *table.TransactionControl
	sessionRunner     yscan.SessionQuerier
	transactionRunner yscan.TransactionQuerier
}

// UseSession run query via session
func UseSession(runner yscan.SessionQuerier) *Executor {
	return &Executor{
		txc:           table.DefaultTxControl(),
		sessionRunner: runner,
	}
}

// UseTransaction run query via transaction
func UseTransaction(runner yscan.TransactionQuerier) *Executor {
	return &Executor{
		transactionRunner: runner,
	}
}

// WithTxControl set table.TransactionControl
func (e *Executor) WithTxControl(txc *table.TransactionControl) *Executor {
	if txc != nil {
		e.txc = txc
	}
	return e
}
