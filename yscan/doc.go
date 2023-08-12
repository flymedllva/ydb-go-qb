// Package `ydbscan` allows scanning data into Go structs and other composite types,
// when working with `ydb` library native interface.
/*
Essentially, `ydbscan` is a wrapper around github.com/georgysavva/scany/v2/dbscan package.
ydbscan connects github.com/ydb-platform/ydb-go-sdk native interface with dbscan functionality.
It contains adapters that are meant to work with result.BaseResult and proxy all calls to dbscan.
ydbscan provides all capabilities available in dbscan.

Querying rows

`ydbscan` can query rows and work with table.TransactionActor, table.Session directly.
To support this it has two high-level functions Select & Get.

`ydbscan` only works with `github.com/ydb-platform/ydb-go-sdk/v3`. So the import path of your ydb must be: "github.com/ydb-platform/ydb-go-sdk/v3".
*/
package yscan
