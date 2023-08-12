# `ydb-go-qb` - query builder for [ydb-go-sdk](https://github.com/ydb-platform/ydb-go-sdk), **not ORM**

[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/ydb-platform/ydb/blob/main/LICENSE)
[![tests](https://github.com/flymedllva/ydb-go-qb/actions/workflows/tests.yml/badge.svg?branch=main)](https://github.com/flymedllva/ydb-go-qb/actions/workflows/tests.yml)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/flymedllva/ydb-go-qb)

## Installation

```sh
go get -u github.com/flymedllva/ydb-go-qb
```

## Packages

* [yscan](yscan) is a `scanner` the [result.Result](https://github.com/ydb-platform/ydb-go-sdk/blob/8e2d8e5196d0e793ad0db0ca4cf75857e0bf5643/table/result/result.go#L122) into Golang struct
  * built on the latest version of [scany](http://github.com/georgysavva/scany)
* [yqb](yqb) is a `query builder` that supports data types for working with the [YDB](https://github.com/ydb-platform/ydb-go-sdk)
  * it's a fork of [squirrel](https://github.com/Masterminds/squirrel) with additional functionality specifically for [YDB](https://github.com/ydb-platform/ydb-go-sdk)
* [yqr](yqr) this is `query runner` connecting [yqb](yqb) & [yscan](yscan) into a convenient API for work with [YDB](https://github.com/ydb-platform/ydb-go-sdk)
* [ysg](ysg) this is `schema generator` connects to YDB, collects circuit information by specified parameters, generates auxiliary go code

## Example

### `yqb & yscan & yqr`

connect using a native API to the [YDB](https://github.com/ydb-platform/ydb-go-sdk)
```go
db, err := ydb.Open(ctx, "grpc://localhost:2136/local")
if err != nil {
    log.Fatal(err)
}
```
execute example `SELECT` query
 ```go
type AwesomeStruct struct {
    ID      string    `db:"id"`
    Pointer *string   `db:"pointer"`
    Int     int       `db:"int"`
    Pi      float64   `db:"pi"`
    Time    time.Time `db:"time"`
    Json    []byte    `db:"json"`
}
var as AwesomeStruct
queryErr := db.Table().Do(ctx, func(ctx context.Context, s table.Session) (err error) {
    query := yqb.Select(
        "\"Hey!\" as id",
        "\"*Hey!\" as pointer",
        "123 as int",
        "Math::Pi() as pi",
        "AddTimezone(CurrentUtcDatetime(), \"Europe/Moscow\") as time",
        "Json(\"{\\\"1\\\": \\\"2\\\"}\") as json",
    )
    
    txc := table.DefaultTxControl()
    tx, err := yqr.ViaSession(txc, s).Get(ctx, &as, query)
    if err != nil {
        return err
    }
    log.Printf("use tx.Rollback or tx.CommitTx if needs from %v\n", tx)
	// or (if txc not commit)
    // r, err := yqr.ViaTransaction(tx).Exec(ctx, yqb.Select("1 as test"))
    // if err != nil {
    // return err
    // }
    // log.Println(r.ResultSetCount())
    
    return nil
})
if queryErr != nil {
    log.Fatal(queryErr)
}
log.Printf("yo %v\n", as)
```

## Features

* Use [yqb](yqb) as you are used to, but with support for YDB variables

```go
yqb.Select("id").
    From("cities").
    Where(yqb.Eq{"city": types.TextValue("Moscow")}).
    OrderBy("city", "number").
    Limit(10)
```
equal
```go
yqb.Select("id").
    From("cities").
    Where(yqb.Eq{"city": "Moscow"}).
    OrderBy("city", "number").
    Limit(10)
```
=>
```sql
DECLARE $p1 AS Utf8;
SELECT id FROM cities WHERE city = $p1 ORDER BY city, number LIMIT 10;
```

* Use complex data types from [YQL](https://ydb.tech/en/docs/yql/reference/)

```go
yqb.Select().
  Column("?", types.StructValue(
        types.StructFieldValue("city", types.TextValue("Moscow")),
        types.StructFieldValue("number", types.Uint32Value(1)), 
      )
  )
```
=>
```sql
DECLARE $p1 AS Struct<'city':Utf8,'number':Uint32>;
SELECT $p1;
```

* Use additional [YDB](https://ydb.tech/) features in your queries

```go
yqb.Select("id").
    From("cities").
    Index("cities_city_idx").
    With("SCHEMA Struct<id:String>").
    Where(yqb.Eq{"city": types.TextValue("Moscow")})
```
=>
```sql
DECLARE $p1 AS Utf8;
SELECT id FROM cities VIEW cities_city_idx WITH SCHEMA Struct<id:String> WHERE city = $p1
```

### `ysg`

```bash
SERVICE_NAME:=auth

ysh --endpoint grpc://localhost:2136 --database /local --service $(SERVICE_NAME)
```
=>

`internal/pkg/storage/schema.go`
```go
package storage

var (
  // usersTable is the table `users`
  usersTable = "`auth/users`"
  // usersTableColumns all columns of the table `users`
  usersTableColumns = []string{"id", "created_at"}
  
  // usersTableIDColumn column `id`
  usersTableIDColumn = "id"
  // usersTableCreatedAtColumn column `created_at`
  usersTableCreatedAtColumn = "created_at"
)

```

## License

Released under the [MIT License](http://www.opensource.org/licenses/MIT). See the bundled [LICENSE](LICENSE) file for details.

## Contributors

All [Contributions](https://github.com/flymedllva/ydb-go-qb/graphs/contributors)

Created by Dmitry Gridnev ([@FlymeDllVa](https://github.com/FlymeDllVa))
