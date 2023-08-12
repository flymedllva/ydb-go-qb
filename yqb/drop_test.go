package yqb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestDropBuilderToSql(t *testing.T) {
	b := Drop("a").
		Prefix("WITH prefix AS ?", 0).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToYdbSql()
	assert.NoError(t, err)

	expectedSql :=
		"DECLARE $p1 AS Int64;\nDECLARE $p2 AS Int64;\n" +
			"WITH prefix AS $p1 DROP TABLE a RETURNING $p2"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", types.Int64Value(0)),
		table.ValueParam("$p2", types.Int64Value(4)),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestDropBuilderToSqlErr(t *testing.T) {
	_, _, err := Drop("").ToSql()
	assert.Error(t, err)
}

func TestDropBuilderPlaceholders(t *testing.T) {
	b := Drop("test").Suffix("SUFFIX x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "DROP TABLE test SUFFIX x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(DollarP).ToSql()
	assert.Equal(t, "DROP TABLE test SUFFIX x = $p1 AND y = $p2", sql)
}

func TestDropBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Drop("test").Suffix("x = ?", 1).RunWith(db)

	expectedSql := "DROP TABLE test x = $p1"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestDropBuilderNoRunner(t *testing.T) {
	b := Drop("test")

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestDropWithQuery(t *testing.T) {
	db := &DBStub{}
	b := Drop("test").Suffix("RETURNING path").RunWith(db)

	expectedSql := "DROP TABLE test RETURNING path"
	b.Query()

	assert.Equal(t, expectedSql, db.LastQuerySql)
}
