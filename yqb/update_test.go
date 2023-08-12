package yqb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestUpdateBuilderToSql(t *testing.T) {
	b := Update("").
		Prefix("WITH prefix AS ?", 0).
		Table("a").
		Set("b", Expr("? + 1", 1)).
		SetMap(Eq{"c": 2}).
		Set("c1", Case("status").When("1", "2").When("2", "1")).
		Set("c2", Case().When("a = 2", Expr("?", "foo")).When("a = 3", Expr("?", "bar"))).
		Set("c3", Select("a").From("b")).
		Where("d = ?", 3).
		OrderBy("e").
		Limit(4).
		Offset(5).
		Suffix("RETURNING ?", 6)

	sql, args, err := b.ToYdbSql()
	assert.NoError(t, err)

	expectedSql :=
		"DECLARE $p1 AS Int64;\nDECLARE $p2 AS Int64;\nDECLARE $p3 AS Int64;\nDECLARE $p4 AS Utf8;\nDECLARE $p5 AS Utf8;\nDECLARE $p6 AS Int64;\nDECLARE $p7 AS Int64;\n" +
			"WITH prefix AS $p1 " +
			"UPDATE a SET b = $p2 + 1, c = $p3, " +
			"c1 = CASE status WHEN 1 THEN 2 WHEN 2 THEN 1 END, " +
			"c2 = CASE WHEN a = 2 THEN $p4 WHEN a = 3 THEN $p5 END, " +
			"c3 = (SELECT a FROM b) " +
			"WHERE d = $p6 " +
			"ORDER BY e LIMIT 4 OFFSET 5 " +
			"RETURNING $p7"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", types.Int64Value(0)),
		table.ValueParam("$p2", types.Int64Value(1)),
		table.ValueParam("$p3", types.Int64Value(2)),
		table.ValueParam("$p4", types.TextValue("foo")),
		table.ValueParam("$p5", types.TextValue("bar")),
		table.ValueParam("$p6", types.Int64Value(3)),
		table.ValueParam("$p7", types.Int64Value(6)),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestUpdateBuilderToSqlErr(t *testing.T) {
	_, _, err := Update("").Set("x", 1).ToSql()
	assert.Error(t, err)

	_, _, err = Update("x").ToYdbSql()
	assert.Error(t, err)
}

func TestUpdateBuilderPlaceholders(t *testing.T) {
	b := Update("test").SetMap(Eq{"x": 1, "y": 2})

	sql, _, _ := b.PlaceholderFormat(Question).ToSql()
	assert.Equal(t, "UPDATE test SET x = ?, y = ?", sql)

	sql, _, _ = b.PlaceholderFormat(Dollar).ToSql()
	assert.Equal(t, "UPDATE test SET x = $1, y = $2", sql)
}

func TestUpdateBuilderRunners(t *testing.T) {
	db := &DBStub{}
	b := Update("test").Set("x", 1).RunWith(db)

	expectedSql := "UPDATE test SET x = $p1"

	b.Exec()
	assert.Equal(t, expectedSql, db.LastExecSql)
}

func TestUpdateBuilderNoRunner(t *testing.T) {
	b := Update("test").Set("x", 1)

	_, err := b.Exec()
	assert.Equal(t, RunnerNotSet, err)
}

func TestUpdateBuilderFrom(t *testing.T) {
	sql, _, err := Update("employees").
		Set("sales_count", 100).
		From("accounts").
		Where("accounts.name = ?", "ACME").ToSql()
	assert.NoError(t, err)
	assert.Equal(t, "UPDATE employees SET sales_count = $p1 FROM accounts WHERE accounts.name = $p2", sql)
}

func TestUpdateBuilderFromSelect(t *testing.T) {
	sql, _, err := Update("employees").
		Set("sales_count", 100).
		FromSelect(Select("id").
			From("accounts").
			Where("accounts.name = ?", "ACME"), "subquery").
		Where("employees.account_id = subquery.id").ToSql()
	assert.NoError(t, err)

	expectedSql :=
		"UPDATE employees " +
			"SET sales_count = $p1 " +
			"FROM (SELECT id FROM accounts WHERE accounts.name = $p2) AS subquery " +
			"WHERE employees.account_id = subquery.id"
	assert.Equal(t, expectedSql, sql)
}
