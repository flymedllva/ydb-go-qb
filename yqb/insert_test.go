package yqb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestInsertBuilderToSql(t *testing.T) {
	b := Insert("").
		Prefix("WITH prefix AS ?", 0).
		Into("a").
		Options("DELAYED", "IGNORE").
		Columns("b", "c").
		Values(1, 2).
		Values(3, Expr("? + 1", 4)).
		Suffix("RETURNING ?", 5)

	sql, args, err := b.ToYdbSql()
	assert.NoError(t, err)

	expectedSQL :=
		"DECLARE $p1 AS Int64;\nDECLARE $p2 AS Int64;\nDECLARE $p3 AS Int64;\nDECLARE $p4 AS Int64;\nDECLARE $p5 AS Int64;\nDECLARE $p6 AS Int64;\n" +
			"WITH prefix AS $p1 " +
			"INSERT DELAYED IGNORE INTO a (b,c) VALUES ($p2,$p3),($p4,$p5 + 1) " +
			"RETURNING $p6"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", types.Int64Value(0)),
		table.ValueParam("$p2", types.Int64Value(1)),
		table.ValueParam("$p3", types.Int64Value(2)),
		table.ValueParam("$p4", types.Int64Value(3)),
		table.ValueParam("$p5", types.Int64Value(4)),
		table.ValueParam("$p6", types.Int64Value(5)),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderToSqlErr(t *testing.T) {
	_, _, err := Insert("").Values(1).ToSql()
	assert.Error(t, err)

	_, _, err = Insert("x").ToSql()
	assert.Error(t, err)
}

func TestInsertBuilderPlaceholders(t *testing.T) {
	b := Insert("test").Values(1, 2)

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES (?,?)", sql)

	sql, _, _ = b.PlaceholderFormat(DollarP).ToSql()
	assert.Equal(t, "INSERT INTO test VALUES ($p1,$p2)", sql)
}

func TestInsertBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Insert("test").Values(1).RunWith(db)

	expectedSQL := "INSERT INTO test VALUES ($p1)"

	b.Exec()
	assert.Equal(t, expectedSQL, db.LastExecSql)
}

func TestInsertBuilderNoRunner(t *testing.T) {
	b := Insert("test").Values(1)

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestInsertBuilderSetMap(t *testing.T) {
	b := Insert("table").
		SetMap(Eq{
			"field1": 1,
			"field2": 2,
			"field3": 3,
		})

	sql, args, err := b.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table (field1,field2,field3) VALUES ($p1,$p2,$p3)"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{types.Int64Value(1), types.Int64Value(2), types.Int64Value(3)}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderSelect(t *testing.T) {
	argOne := types.Int64Value(1)

	sb := Select("field1").From("table1").Where(Eq{"field1": 1})
	ib := Insert("table2").Columns("field1").Select(sb)

	sql, args, err := ib.ToSql()
	assert.NoError(t, err)

	expectedSQL := "INSERT INTO table2 (field1) SELECT field1 FROM table1 WHERE field1 = $p1"
	assert.Equal(t, expectedSQL, sql)

	expectedArgs := []any{argOne}
	assert.Equal(t, expectedArgs, args)
}

func TestInsertBuilderReplace(t *testing.T) {
	b := Replace("table").Values(1)

	expectedSQL := "REPLACE INTO table VALUES ($p1)"

	sql, _, err := b.ToSql()
	assert.NoError(t, err)

	assert.Equal(t, expectedSQL, sql)
}
