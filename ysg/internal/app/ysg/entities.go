package ysg

import (
	"fmt"
	"strings"
)

type Schema struct {
	// Service название сервиса, например `users`
	Service string
	// Tables таблицы
	Tables []Table
}

type Table struct {
	// Service название сервиса, например `users`
	Service string
	// Name название, например `users`
	Name string
	// Columns колонки
	Columns []Column
}

func (t Table) GoNamePrivate() string {
	return goName(t.Name, false)
}

func (t Table) GoNamePublic() string {
	return goName(t.Name, true)
}

func (t Table) ColumnsString() string {
	allColumns := make([]string, len(t.Columns))
	for i, column := range t.Columns {
		allColumns[i] = fmt.Sprintf("\"%s\"", column.Name)
	}

	return fmt.Sprintf("[]string{%s}", strings.Join(allColumns, ", "))
}

type Column struct {
	// Name название, например `id`
	Name string
	// TableName название таблицы, например `users`
	TableName string
}

func (c Column) GoNamePrivate() string {
	return goName(c.Name, false)
}

func (c Column) GoNamePublic() string {
	return goName(c.Name, true)
}
