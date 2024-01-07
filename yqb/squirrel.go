package yqb

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/lann/builder"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

// Sqlizer is the interface that wraps the ToSql method.
//
// ToSql returns a SQL representation of the Sqlizer, along with a slice of args
// as passed to e.g. database/sql.Exec. It can also return an error.
type Sqlizer interface {
	ToSql() (string, []any, error)
}

// YdbSqlizer is the interface that wraps the ToYdbSql method.
type YdbSqlizer interface {
	ToYdbSql() (string, []table.ParameterOption, error)
}

// rawSqlizer is expected to do what Sqlizer does, but without finalizing placeholders.
// This is useful for nested queries.
type rawSqlizer interface {
	toSqlRaw() (string, []any, error)
}

// Execer is the interface that wraps the Exec method.
//
// Exec executes the given query as implemented by database/sql.Exec.
type Execer interface {
	Exec(query string, args ...any) (sql.Result, error)
}

// Queryer is the interface that wraps the Query method.
//
// Query executes the given query as implemented by database/sql.Query.
type Queryer interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

// QueryRower is the interface that wraps the QueryRow method.
//
// QueryRow executes the given query as implemented by database/sql.QueryRow.
type QueryRower interface {
	QueryRow(query string, args ...any) RowScanner
}

// BaseRunner groups the Execer and Queryer interfaces.
type BaseRunner interface {
	Execer
	Queryer
}

// Runner groups the Execer, Queryer, and QueryRower interfaces.
type Runner interface {
	Execer
	Queryer
	QueryRower
}

// WrapStdSql wraps a type implementing the standard SQL interface with methods that
// squirrel expects.
func WrapStdSql(stdSql StdSql) Runner {
	return &stdsqlRunner{stdSql}
}

// StdSql encompasses the standard methods of the *sql.DB type, and other types that
// wrap these methods.
type StdSql interface {
	Query(string, ...any) (*sql.Rows, error)
	QueryRow(string, ...any) *sql.Row
	Exec(string, ...any) (sql.Result, error)
}

type stdsqlRunner struct {
	StdSql
}

func (r *stdsqlRunner) QueryRow(query string, args ...any) RowScanner {
	return r.StdSql.QueryRow(query, args...)
}

func setRunWith(b any, runner BaseRunner) any {
	switch r := runner.(type) {
	case StdSql:
		runner = WrapStdSql(r)
	}
	return builder.Set(b, "RunWith", runner)
}

// RunnerNotSet is returned by methods that need a Runner if it isn't set.
var RunnerNotSet = fmt.Errorf("cannot run; no Runner set (RunWith)")

// RunnerNotQueryRunner is returned by QueryRow if the RunWith value doesn't implement QueryRower.
var RunnerNotQueryRunner = fmt.Errorf("cannot QueryRow; Runner is not a QueryRower")

// ExecWith Execs the SQL returned by s with db.
func ExecWith(db Execer, s Sqlizer) (res sql.Result, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.Exec(query, args...)
}

// QueryWith Querys the SQL returned by s with db.
func QueryWith(db Queryer, s Sqlizer) (rows *sql.Rows, err error) {
	query, args, err := s.ToSql()
	if err != nil {
		return
	}
	return db.Query(query, args...)
}

// QueryRowWith QueryRows the SQL returned by s with db.
func QueryRowWith(db QueryRower, s Sqlizer) RowScanner {
	query, args, err := s.ToSql()
	return &Row{RowScanner: db.QueryRow(query, args...), err: err}
}

func prepareYdbSqlString(sql string, args []any) (string, error) {
	var sb strings.Builder
	for i, arg := range args {
		ydbArg, ok := arg.(types.Value)
		if !ok {
			return "", fmt.Errorf("arg %T is not ydb.Value", arg)
		}
		sb.WriteString(fmt.Sprintf("DECLARE $p%d AS ", i+1))
		sb.WriteString(ydbArg.Type().Yql())
		sb.WriteString(";\n")
	}
	sb.WriteString(sql)

	return sb.String(), nil
}

func prepareYdbParams(args []any) ([]table.ParameterOption, error) {
	ydbArgs := make([]table.ParameterOption, 0, len(args))
	for i, arg := range args {
		ydbArgValue, ok := arg.(types.Value)
		if !ok {
			return nil, fmt.Errorf("arg %T is not ydb.Value", arg)
		}
		ydbArgs = append(ydbArgs, table.ValueParam(
			fmt.Sprintf("p%d", i+1), ydbArgValue,
		))
	}

	return ydbArgs, nil
}

func castArgsToYdb(args []any) ([]any, error) {
	if len(args) == 0 {
		return []any(nil), nil
	}

	ydbArgs := make([]any, 0, len(args))
	for _, arg := range args {
		switch arg.(type) {
		case types.Value:
			ydbArgs = append(ydbArgs, arg)
		default:
			castedYdbArgs, err := castArgToYdb(arg)
			if err != nil {
				return nil, fmt.Errorf("castArgToYdb: %w", err)
			}
			for _, ydbArg := range castedYdbArgs {
				ydbArgs = append(ydbArgs, ydbArg)
			}
		}
	}

	return ydbArgs, nil
}

func castArgToYdb(arg any) ([]types.Value, error) {
	switch t := arg.(type) {
	case bool:
		return []types.Value{
			types.BoolValue(t),
		}, nil
	case *bool:
		return []types.Value{
			types.NullableBoolValue(t),
		}, nil
	case int:
		return []types.Value{
			types.Int64Value(int64(t)),
		}, nil
	case *int:
		tt := int64(*t)
		return []types.Value{
			types.NullableInt64Value(&tt),
		}, nil
	case int8:
		return []types.Value{
			types.Int8Value(t),
		}, nil
	case *int8:
		return []types.Value{
			types.NullableInt8Value(t),
		}, nil
	case int16:
		return []types.Value{
			types.Int16Value(t),
		}, nil
	case *int16:
		return []types.Value{
			types.NullableInt16Value(t),
		}, nil
	case int32:
		return []types.Value{
			types.Int32Value(t),
		}, nil
	case *int32:
		return []types.Value{
			types.NullableInt32Value(t),
		}, nil
	case int64:
		return []types.Value{
			types.Int64Value(t),
		}, nil
	case *int64:
		return []types.Value{
			types.NullableInt64Value(t),
		}, nil
	case uint:
		return []types.Value{
			types.Uint64Value(uint64(t)),
		}, nil
	case *uint:
		tt := uint64(*t)
		return []types.Value{
			types.NullableUint64Value(&tt),
		}, nil
	case uint8:
		return []types.Value{
			types.Uint8Value(t),
		}, nil
	case *uint8:
		return []types.Value{
			types.NullableUint8Value(t),
		}, nil
	case uint16:
		return []types.Value{
			types.Uint16Value(t),
		}, nil
	case *uint16:
		return []types.Value{
			types.NullableUint16Value(t),
		}, nil
	case uint32:
		return []types.Value{
			types.Uint32Value(t),
		}, nil
	case *uint32:
		return []types.Value{
			types.NullableUint32Value(t),
		}, nil
	case uint64:
		return []types.Value{
			types.Uint64Value(t),
		}, nil
	case *uint64:
		return []types.Value{
			types.NullableUint64Value(t),
		}, nil
	case float32:
		return []types.Value{
			types.FloatValue(t),
		}, nil
	case *float32:
		return []types.Value{
			types.NullableFloatValue(t),
		}, nil
	case float64:
		return []types.Value{
			types.DoubleValue(t),
		}, nil
	case *float64:
		return []types.Value{
			types.NullableDoubleValue(t),
		}, nil
	case string:
		return []types.Value{
			types.TextValue(t),
		}, nil
	case *string:
		return []types.Value{
			types.NullableTextValue(t),
		}, nil
	case []byte:
		return []types.Value{
			types.BytesValue(t),
		}, nil
	case time.Time:
		return []types.Value{
			types.TimestampValueFromTime(t),
		}, nil
	case *time.Time:
		return []types.Value{
			types.NullableTimestampValueFromTime(t),
		}, nil
	case json.RawMessage:
		if t == nil {
			return []types.Value{
				types.NullableJSONValueFromBytes(nil),
			}, nil
		}
		return []types.Value{
			types.JSONValueFromBytes(t),
		}, nil
	default:
		return nil, fmt.Errorf("unsupported type `%T`", arg)
	}
}
