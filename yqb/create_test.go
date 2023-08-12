package yqb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestCreateBuilderToSql(t *testing.T) {
	b := Create("test").
		Prefix("WITH prefix AS ?", 0).
		Columns("id", "a", "b").
		Types("INT", "TEXT", "DATE").
		PrimaryKey("id").
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToYdbSql()
	assert.NoError(t, err)

	expectedSql :=
		"DECLARE $p1 AS Int64;\nDECLARE $p2 AS Int64;\n" +
			"WITH prefix AS $p1 " +
			"CREATE TABLE test ( id INT, a TEXT, b DATE, PRIMARY KEY (id) ) " +
			"RETURNING $p2"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", types.Int64Value(0)),
		table.ValueParam("$p2", types.Int64Value(4)),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCreateBuilderSetMapToSql(t *testing.T) {
	b := Create("test").
		SetMap(map[string]string{
			"a": "INT",
			"b": "TEXT",
			"c": "DATE",
			"d": "TIMESTAMP",
		})

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	expectedSql := "CREATE TABLE test ( a INT, b TEXT, c DATE, d TIMESTAMP )"
	assert.Equal(t, expectedSql, sql)
}

func TestCreateBuilderToSqlErr(t *testing.T) {
	_, _, err := Create("").ToSql()
	assert.Error(t, err)
}

func TestCreateBuilderPlaceholders(t *testing.T) {
	b := Create("test").Columns("id", "a", "b").
		Types("INT", "TEXT", "DATE").Suffix("SUFFIX x = ? AND y = ?")

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "CREATE TABLE test ( id INT, a TEXT, b DATE ) SUFFIX x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(DollarP).ToSql()
	assert.Equal(t, "CREATE TABLE test ( id INT, a TEXT, b DATE ) SUFFIX x = $p1 AND y = $p2", sql)
}

func TestCreateBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Create("test").Columns("id", "a", "b").
		Types("INT", "TEXT", "DATE").Suffix("SUFFIX x = ?").RunWith(db)

	expectedSql := "CREATE TABLE test ( id INT, a TEXT, b DATE ) SUFFIX x = $p1"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestCreateBuilderNoRunner(t *testing.T) {
	b := Create("test")

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestCreateWithQuery(t *testing.T) {
	db := &DBStub{}
	b := Create("test").Columns("id", "a", "b").
		Types("INT", "TEXT", "DATE").Suffix("RETURNING path").RunWith(db)

	expectedSql := "CREATE TABLE test ( id INT, a TEXT, b DATE ) RETURNING path"
	b.Query()

	assert.Equal(t, expectedSql, db.LastQuerySql)
}
