package yqb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestDeleteBuilderToSql(t *testing.T) {
	b := Delete("").
		Prefix("WITH prefix AS ?", 0).
		From("a").
		Where("b = ?", 1).
		OrderBy("c").
		Limit(2).
		Offset(3).
		Suffix("RETURNING ?", 4)

	sql, args, err := b.ToYdbSql()
	assert.NoError(t, err)

	expectedSql :=
		"DECLARE $p1 AS Int64;\nDECLARE $p2 AS Int64;\nDECLARE $p3 AS Int64;\n" +
			"WITH prefix AS $p1 " +
			"DELETE FROM a WHERE b = $p2 ORDER BY c LIMIT 2 OFFSET 3 " +
			"RETURNING $p3"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", types.Int64Value(0)),
		table.ValueParam("$p2", types.Int64Value(1)),
		table.ValueParam("$p3", types.Int64Value(4)),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestDeleteBuilderToSqlErr(t *testing.T) {
	_, _, err := Delete("").ToSql()
	assert.Error(t, err)
}

func TestDeleteBuilderPlaceholders(t *testing.T) {
	b := Delete("test").Where("x = ? AND y = ?", 1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = ? AND y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(DollarP).ToSql()
	assert.Equal(t, "DELETE FROM test WHERE x = $p1 AND y = $p2", sql)
}

func TestDeleteBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Delete("test").Where("x = ?", 1).RunWith(db)

	expectedSql := "DELETE FROM test WHERE x = $p1"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestDeleteBuilderNoRunner(t *testing.T) {
	b := Delete("test")

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestDeleteWithQuery(t *testing.T) {
	db := &DBStub{}
	b := Delete("test").Where("id=55").Suffix("RETURNING path").RunWith(db)

	expectedSql := "DELETE FROM test WHERE id=55 RETURNING path"
	b.Query()

	assert.Equal(t, expectedSql, db.LastQuerySql)
}
