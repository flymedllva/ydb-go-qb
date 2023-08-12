package ysg

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"strings"
	"text/template"

	"github.com/flymedllva/ydb-go-qb/ysg/internal/pkg"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// Service сервис
type Service struct {
	ydbConn *ydb.Driver
}

// NewService создает новый сервис
func NewService(ydbConn *ydb.Driver) *Service {
	return &Service{ydbConn: ydbConn}
}

// GoName имя в стиле Go
func goName(name string, initUpper bool) string {
	out := ""
	for i, p := range strings.Split(name, "_") {
		if !initUpper && i == 0 {
			out += p
			continue
		}
		if p == "id" {
			out += "ID"
		} else {
			out += cases.Title(language.English).String(p)
		}
	}
	return out
}

// Generate генерирует код для сервиса
func (s *Service) Generate(ctx context.Context, service string) ([]byte, error) {
	tables, err := s.describeTables(ctx, service)
	if err != nil {
		return nil, fmt.Errorf("s.describeTables: %w", err)
	}
	schema := s.tablesSchema(service, tables)

	funcMap := template.FuncMap{}
	tmpl := template.Must(
		template.New("templates").
			Funcs(funcMap).
			ParseFS(
				pkg.Templates,
				"templates/*.tmpl",
			),
	)

	var b bytes.Buffer
	w := bufio.NewWriter(&b)
	err = tmpl.ExecuteTemplate(w, "schema.tmpl", &schema)
	_ = w.Flush()
	if err != nil {
		return nil, fmt.Errorf("ExecuteTemplate modelsSchema: %w", err)
	}

	return b.Bytes(), nil
}

func (s *Service) tablesSchema(service string, tablesDescription []options.Description) Schema {
	schema := Schema{
		Service: service,
	}
	for _, tableDescription := range tablesDescription {
		t := Table{
			Service: service,
			Name:    tableDescription.Name,
			Columns: nil,
		}
		var columns []Column
		for _, columnDescription := range tableDescription.Columns {
			c := Column{
				Name:      columnDescription.Name,
				TableName: tableDescription.Name,
			}
			columns = append(columns, c)
		}
		t.Columns = columns
		schema.Tables = append(schema.Tables, t)
	}

	return schema
}

func (s *Service) describeTables(ctx context.Context, service string) ([]options.Description, error) {
	serviceDirectoryPath := fmt.Sprintf("%s/%s", s.ydbConn.Scheme().Database(), service)
	serviceDirectory, err := s.ydbConn.Scheme().ListDirectory(ctx, serviceDirectoryPath)
	if err != nil {
		return nil, fmt.Errorf("ydbConn.Scheme().ListDirectory %q: %w", serviceDirectoryPath, err)
	}

	var serviceTables []string
	for _, child := range serviceDirectory.Children {
		if !child.IsTable() {
			continue
		}
		serviceTables = append(serviceTables, child.Name)
	}

	var serviceTableDescriptions []options.Description
	err = s.ydbConn.Table().Do(context.Background(), func(ctx context.Context, s table.Session) (err error) {
		for _, serviceTable := range serviceTables {
			serviceTablePath := fmt.Sprintf("%s/%s", serviceDirectoryPath, serviceTable)

			var serviceTableDescription options.Description
			serviceTableDescription, err = s.DescribeTable(ctx, serviceTablePath)
			if err != nil {
				return fmt.Errorf("s.DescribeTable %q: %w", serviceTablePath, err)
			}
			serviceTableDescriptions = append(serviceTableDescriptions, serviceTableDescription)
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("ydbConn.Table().Do: %w", err)
	}

	return serviceTableDescriptions, nil
}
