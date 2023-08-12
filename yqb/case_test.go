package yqb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func TestCaseWithVal(t *testing.T) {
	arg := "big number"
	caseStmt := Case("number").
		When("1", "one").
		When("2", "two").
		Else(Expr("?", arg))

	qb := Select().
		Column(caseStmt).
		From("table")
	sql, args, err := qb.ToYdbSql()

	assert.NoError(t, err)

	expectedSql := "DECLARE $p1 AS Utf8;\n" +
		"SELECT CASE number " +
		"WHEN 1 THEN one " +
		"WHEN 2 THEN two " +
		"ELSE $p1 " +
		"END " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", types.UTF8Value(arg)),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithComplexVal(t *testing.T) {
	argOne := types.Int64Value(1)
	argTwo := types.Int64Value(2)

	caseStmt := Case("? > ?", 1, 2).
		When("true", "'T'")

	qb := Select().
		Column(Alias(caseStmt, "complexCase")).
		From("table")
	sql, args, err := qb.ToYdbSql()

	assert.NoError(t, err)

	expectedSql := "DECLARE $p1 AS Int64;\nDECLARE $p2 AS Int64;\n" +
		"SELECT (CASE $p1 > $p2 " +
		"WHEN true THEN 'T' " +
		"END) AS complexCase " +
		"FROM table"
	assert.Equal(t, expectedSql, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", argOne),
		table.ValueParam("$p2", argTwo),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoVal(t *testing.T) {
	argZero := types.Int64Value(0)
	argOne := types.Int64Value(1)
	argTwo := types.Int64Value(2)

	caseStmt := Case().
		When(Eq{"x": 0}, "x is zero").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToYdbSql()

	assert.NoError(t, err)

	expectedSql := "DECLARE $p1 AS Int64;\nDECLARE $p2 AS Int64;\nDECLARE $p3 AS Int64;\n" +
		"SELECT CASE " +
		"WHEN x = $p1 THEN x is zero " +
		"WHEN x > $p2 THEN CONCAT('x is greater than ', $p3) " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", argZero),
		table.ValueParam("$p2", argOne),
		table.ValueParam("$p3", argTwo),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithExpr(t *testing.T) {
	argBool := types.BoolValue(true)
	argString := types.UTF8Value("it's true!")

	caseStmt := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")

	qb := Select().Column(caseStmt).From("table")
	sql, args, err := qb.ToYdbSql()

	assert.NoError(t, err)

	expectedSql := "DECLARE $p1 AS Bool;\nDECLARE $p2 AS Utf8;\n" +
		"SELECT CASE x = $p1 " +
		"WHEN true THEN $p2 " +
		"ELSE 42 " +
		"END " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", argBool),
		table.ValueParam("$p2", argString),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestMultipleCase(t *testing.T) {
	argBool := types.BoolValue(true)
	argString := types.UTF8Value("it's true!")
	argZero := types.Int64Value(0)
	argOne := types.Int64Value(1)
	argTwo := types.Int64Value(2)

	caseStmtNoval := Case(Expr("x = ?", true)).
		When("true", Expr("?", "it's true!")).
		Else("42")
	caseStmtExpr := Case().
		When(Eq{"x": 0}, "'x is zero'").
		When(Expr("x > ?", 1), Expr("CONCAT('x is greater than ', ?)", 2))

	qb := Select().
		Column(Alias(caseStmtNoval, "case_noval")).
		Column(Alias(caseStmtExpr, "case_expr")).
		From("table")

	sql, args, err := qb.ToYdbSql()

	assert.NoError(t, err)

	expectedSql := "DECLARE $p1 AS Bool;\nDECLARE $p2 AS Utf8;\nDECLARE $p3 AS Int64;\nDECLARE $p4 AS Int64;\nDECLARE $p5 AS Int64;\n" +
		"SELECT " +
		"(CASE x = $p1 WHEN true THEN $p2 ELSE 42 END) AS case_noval, " +
		"(CASE WHEN x = $p3 THEN 'x is zero' WHEN x > $p4 THEN CONCAT('x is greater than ', $p5) END) AS case_expr " +
		"FROM table"

	assert.Equal(t, expectedSql, sql)

	expectedArgs := []table.ParameterOption{
		table.ValueParam("$p1", argBool),
		table.ValueParam("$p2", argString),
		table.ValueParam("$p3", argZero),
		table.ValueParam("$p4", argOne),
		table.ValueParam("$p5", argTwo),
	}
	assert.Equal(t, expectedArgs, args)
}

func TestCaseWithNoWhenClause(t *testing.T) {
	caseStmt := Case("something").
		Else("42")

	qb := Select().Column(caseStmt).From("table")

	_, _, err := qb.ToSql()

	assert.Error(t, err)

	assert.Equal(t, "case expression must contain at lease one WHEN clause", err.Error())
}
