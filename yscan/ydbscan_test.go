package yscan_test

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"github.com/flymedllva/ydb-go-qb/yscan"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
)

var (
	testDB  *ydb.Driver
	testAPI *yscan.API

	ctx = context.Background()
	txc = table.TxControl(table.BeginTx(table.WithOnlineReadOnly()), table.CommitTx())
)

type testModel struct {
	Foo string
	Bar string
}

const (
	multipleRowsQuery = `
		SELECT *
		FROM (
			VALUES ('foo val', 'bar val'), ('foo val 2', 'bar val 2'), ('foo val 3', 'bar val 3')
		) AS t (foo, bar)
	`
	singleRowsQuery = `
		SELECT 'foo val' AS foo, 'bar val' AS bar
	`
	noRowsQuery = `
		SELECT NULL AS foo, NULL AS bar LIMIT 0;
    `
)

func TestSelect(t *testing.T) {
	t.Parallel()
	expected := []*testModel{
		{Foo: "foo val", Bar: "bar val"},
		{Foo: "foo val 2", Bar: "bar val 2"},
		{Foo: "foo val 3", Bar: "bar val 3"},
	}

	var got []*testModel
	err := testDB.Table().Do(ctx, func(ctx context.Context, tsc table.Session) error {
		_, err := testAPI.Select(ctx, txc, tsc, &got, multipleRowsQuery, nil)
		require.NoError(t, err)
		assert.Equal(t, expected, got)
		return nil
	})
	require.NoError(t, err)
}

func TestSelect_queryError_propagatesAndWrapsErr(t *testing.T) {
	t.Parallel()
	query := `
		SELECT foo, bar, baz
		FROM (
			VALUES ('foo val', 'bar val'), ('foo val 2', 'bar val 2'), ('foo val 3', 'bar val 3')
		) AS t (foo, bar)
	`
	expectedErr := "Member not found: baz. Did you mean bar?"

	dst := &[]*testModel{}
	err := testDB.Table().Do(ctx, func(ctx context.Context, tsc table.Session) error {
		_, err := testAPI.Select(ctx, txc, tsc, dst, query, nil)
		assert.Contains(t, err.Error(), expectedErr)

		return nil
	})
	require.NoError(t, err)
}

func TestGet(t *testing.T) {
	t.Parallel()
	expected := testModel{Foo: "foo val", Bar: "bar val"}

	var got testModel
	err := testDB.Table().Do(ctx, func(ctx context.Context, tsc table.Session) error {
		_, err := testAPI.Get(ctx, txc, tsc, &got, singleRowsQuery, nil)
		require.NoError(t, err)
		assert.Equal(t, expected, got)

		return nil
	})
	require.NoError(t, err)
}

func TestGet_queryError_propagatesAndWrapsErr(t *testing.T) {
	t.Parallel()
	query := `
		SELECT 'foo val' AS foo, 'bar val' AS bar, baz
	`
	expectedErr := "Column references are not allowed without FROM"

	dst := &testModel{}
	err := testDB.Table().Do(ctx, func(ctx context.Context, tsc table.Session) error {
		_, err := testAPI.Get(ctx, txc, tsc, dst, query, nil)
		assert.Contains(t, err.Error(), expectedErr)

		return nil
	})
	require.NoError(t, err)
}

func getAPI() (*yscan.API, error) {
	dbscanAPI, err := yscan.NewDBScanAPI()
	if err != nil {
		return nil, fmt.Errorf("new DB scan API: %w", err)
	}
	api, err := yscan.NewAPI(dbscanAPI)
	if err != nil {
		return nil, fmt.Errorf("new API: %w", err)
	}
	return api, nil
}

func TestMain(m *testing.M) {
	exitCode := func() int {
		flag.Parse()

		var err error
		testDB, err = ydb.Open(ctx, "grpc://localhost:2136/local")
		if err != nil {
			panic(err)
		}
		defer testDB.Close(ctx)

		testAPI, err = getAPI()
		if err != nil {
			panic(err)
		}

		return m.Run()
	}()
	os.Exit(exitCode)
}
