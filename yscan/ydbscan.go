package yscan

import (
	"context"
	"fmt"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

// SessionQuerier is an interface that is implemented by table.Session.
type SessionQuerier interface {
	Execute(
		ctx context.Context,
		tx *table.TransactionControl,
		query string,
		params *table.QueryParameters,
		opts ...options.ExecuteDataQueryOption,
	) (table.Transaction, result.Result, error)
}

var (
	_ SessionQuerier = table.Session(nil)
)

// TransactionQuerier is an interface that is implemented by table.TransactionActor.
type TransactionQuerier interface {
	Execute(
		ctx context.Context,
		query string,
		params *table.QueryParameters,
		opts ...options.ExecuteDataQueryOption,
	) (result.Result, error)
}

var (
	_ TransactionQuerier = table.TransactionActor(nil)
)

// RowScanner is a wrapper around the dbscan.RowScanner type.
// See dbscan.RowScanner for details.
type RowScanner struct {
	*dbscan.RowScanner
}

// NewRowScanner is a package-level helper function that uses the DefaultAPI object.
// See API.NewRowScanner for details.
func NewRowScanner(result result.Result) *RowScanner {
	return DefaultAPI.NewRowScanner(result)
}

// ScanRow is a package-level helper function that uses the DefaultAPI object.
// See API.ScanRow for details.
func ScanRow(dst any, result result.Result) error {
	return DefaultAPI.ScanRow(dst, result)
}

// NewDBScanAPI creates a new dbscan API object with default configuration settings for pgxscan.
func NewDBScanAPI(opts ...dbscan.APIOption) (*dbscan.API, error) {
	var defaultOpts []dbscan.APIOption
	opts = append(defaultOpts, opts...)

	return dbscan.NewAPI(opts...)
}

// API is a wrapper around the dbscan.API type.
// See dbscan.API for details.
type API struct {
	dbscanAPI *dbscan.API
}

// NewAPI creates new API instance from dbscan.API instance.
func NewAPI(dbscanAPI *dbscan.API) (*API, error) {
	return &API{
		dbscanAPI: dbscanAPI,
	}, nil
}

// Select is a high-level function that queries rows from Querier and calls the ScanAll function.
// See ScanAll for details.
func (a *API) Select(ctx context.Context, tx *table.TransactionControl, db SessionQuerier, dst any, query string, params *table.QueryParameters) (table.Transaction, error) {
	trx, r, err := db.Execute(ctx, tx, query, params)
	if err != nil {
		return trx, fmt.Errorf("scany: query multiple result rows: %w", err)
	}
	defer func() {
		resultErr := r.Close()
		if resultErr != nil {
			err = fmt.Errorf("result.Close: %w: %w", resultErr, err)
		}
	}()

	if err = r.NextResultSetErr(ctx); err != nil {
		return trx, fmt.Errorf("result.NextResultSetErr: %w", err)
	}
	if err = a.ScanAll(dst, r); err != nil {
		return trx, fmt.Errorf("scanning all: %w", err)
	}

	return trx, nil
}

// SelectTx is a high-level function that queries rows from Querier and calls the ScanAll function.
// See ScanAll for details.
func (a *API) SelectTx(ctx context.Context, db TransactionQuerier, dst any, query string, params *table.QueryParameters) error {
	r, err := db.Execute(ctx, query, params)
	if err != nil {
		return fmt.Errorf("scany: query multiple result rows: %w", err)
	}
	defer func() {
		resultErr := r.Close()
		if resultErr != nil {
			err = fmt.Errorf("result.Close: %w: %w", resultErr, err)
		}
	}()

	if err = r.NextResultSetErr(ctx); err != nil {
		return fmt.Errorf("result.NextResultSetErr: %w", err)
	}
	if err = a.ScanAll(dst, r); err != nil {
		return fmt.Errorf("scanning all: %w", err)
	}

	return nil
}

// Get is a high-level function that queries rows from Querier and calls the ScanOne function.
// See ScanOne for details.
func (a *API) Get(ctx context.Context, tx *table.TransactionControl, db SessionQuerier, dst any, query string, params *table.QueryParameters) (table.Transaction, error) {
	trx, r, err := db.Execute(ctx, tx, query, params)
	if err != nil {
		return trx, fmt.Errorf("scany: query one result row: %w", err)
	}
	defer func() {
		resultErr := r.Close()
		if resultErr != nil {
			err = fmt.Errorf("result.Close: %w: %w", resultErr, err)
		}
	}()

	if err = r.NextResultSetErr(ctx); err != nil {
		return trx, fmt.Errorf("result.NextResultSetErr: %w", err)
	}
	if err = a.ScanOne(dst, r); err != nil {
		return trx, fmt.Errorf("scanning one: %w", err)
	}

	return trx, nil
}

// GetTx is a high-level function that queries rows from Querier and calls the ScanOne function.
// See ScanOne for details.
func (a *API) GetTx(ctx context.Context, db TransactionQuerier, dst any, query string, params *table.QueryParameters) error {
	r, err := db.Execute(ctx, query, params)
	if err != nil {
		return fmt.Errorf("scany: query one result row: %w", err)
	}
	defer func() {
		resultErr := r.Close()
		if resultErr != nil {
			err = fmt.Errorf("result.Close: %w: %w", resultErr, err)
		}
	}()

	if err = r.NextResultSetErr(ctx); err != nil {
		return fmt.Errorf("result.NextResultSetErr: %w", err)
	}
	if err = a.ScanOne(dst, r); err != nil {
		return fmt.Errorf("scanning one: %w", err)
	}

	return nil
}

// ScanAll is a wrapper around the dbscan.ScanAll function.
// See dbscan.ScanAll for details.
func (a *API) ScanAll(dst any, result result.BaseResult) error {
	return a.dbscanAPI.ScanAll(dst, NewRowsAdapter(result))
}

// ScanOne is a wrapper around the dbscan.ScanOne function.
// See dbscan.ScanOne for details. If no rows are found it
// returns a pgx.ErrNoRows error.
func (a *API) ScanOne(dst any, result result.BaseResult) error {
	switch err := a.dbscanAPI.ScanOne(dst, NewRowsAdapter(result)); {
	case dbscan.NotFound(err):
		return fmt.Errorf("%w: %v", ErrNoRows, err)
	case err != nil:
		return fmt.Errorf("%w", err)
	default:
		return nil
	}
}

// NewRowScanner returns a new RowScanner instance.
func (a *API) NewRowScanner(rows result.BaseResult) *RowScanner {
	ra := NewRowsAdapter(rows)
	return &RowScanner{RowScanner: a.dbscanAPI.NewRowScanner(ra)}
}

// ScanRow is a wrapper around the dbscan.ScanRow function.
// See dbscan.ScanRow for details.
func (a *API) ScanRow(dst any, rows result.BaseResult) error {
	return a.dbscanAPI.ScanRow(dst, NewRowsAdapter(rows))
}

// RowsAdapter makes pgx.Rows compliant with the dbscan.Rows interface.
// See dbscan.Rows for details.
type RowsAdapter struct {
	result result.BaseResult
}

// NewRowsAdapter returns a new RowsAdapter instance.
func NewRowsAdapter(result result.BaseResult) *RowsAdapter {
	return &RowsAdapter{result: result}
}

// Columns implements the dbscan.Rows.Columns method.
func (ra RowsAdapter) Columns() ([]string, error) {
	columns := make([]string, 0, ra.result.CurrentResultSet().ColumnCount())
	ra.result.CurrentResultSet().Columns(func(column options.Column) {
		columns = append(columns, column.Name)
	})

	return columns, nil
}

// Close implements the dbscan.Rows.Close method.
func (ra RowsAdapter) Close() error {
	return ra.result.Close()
}

// Err return scanner error
// To handle errors, do not need to check after scanning each row
// It is enough to check after reading all Set
func (ra RowsAdapter) Err() error {
	return ra.result.Err()
}

// Next selects row in the current result set.
// It returns false if there are no more rows in the result set.
func (ra RowsAdapter) Next() bool {
	return ra.result.NextRow()
}

// Scan scans row with column names defined in namedValues
func (ra RowsAdapter) Scan(dest ...any) error {
	n := make([]named.Value, 0, ra.result.CurrentResultSet().ColumnCount())
	i := -1
	ra.result.CurrentResultSet().Columns(func(column options.Column) {
		i++
		if isDoublePointer(dest[i]) {
			n = append(n, named.Optional(column.Name, dest[i]))
		} else {
			n = append(n, named.OptionalWithDefault(column.Name, dest[i]))
		}
	})
	err := ra.result.ScanNamed(n...)
	if err != nil {
		return err
	}

	return nil
}

func mustNewDBScanAPI(opts ...dbscan.APIOption) *dbscan.API {
	api, err := NewDBScanAPI(opts...)
	if err != nil {
		panic(err)
	}
	return api
}

func mustNewAPI(dbscanAPI *dbscan.API) *API {
	api, err := NewAPI(dbscanAPI)
	if err != nil {
		panic(err)
	}
	return api
}

// DefaultAPI is the default instance of API with all configuration settings set to default.
var DefaultAPI = mustNewAPI(mustNewDBScanAPI())
