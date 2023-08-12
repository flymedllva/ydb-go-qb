package main

import (
	"fmt"
	"os"

	"github.com/flymedllva/ydb-go-qb/ysg/internal/app/ysg"
	"github.com/urfave/cli/v2"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

const (
	version = "0.1"
)

func main() {
	app := cli.NewApp()
	app.Name = "ysh"
	app.Usage = generateCmd.Usage
	app.Description = "Generate schema file from YDB."
	app.HideVersion = true
	app.Flags = generateCmd.Flags
	app.Version = version

	app.Action = generateCmd.Action
	app.Commands = []*cli.Command{
		generateCmd,
	}

	if err := app.Run(os.Args); err != nil {
		fmt.Fprint(os.Stderr, err.Error()+"\n")
		os.Exit(1)
	}
}

var generateCmd = &cli.Command{
	Name:  "generate",
	Usage: "generate schema from YDB",
	Flags: []cli.Flag{
		&cli.StringFlag{Name: "endpoint, e", Usage: "grpc://localhost:2136"},
		&cli.StringFlag{Name: "service, s", Usage: "auth/user/etc"},
		&cli.StringFlag{Name: "database, d", Usage: "/local"},
	},
	Action: func(cliCtx *cli.Context) error {
		endpoint := cliCtx.String("endpoint")
		if endpoint == "" {
			return fmt.Errorf("endpoint is required")
		}
		service := cliCtx.String("service")
		if service == "" {
			return fmt.Errorf("service is required")
		}
		database := cliCtx.String("database")
		if database == "" {
			return fmt.Errorf("database is required")
		}

		ydbConn, err := ydb.Open(cliCtx.Context, endpoint+database)
		if err != nil {
			return fmt.Errorf("ydb.Open: %w", err)
		}
		defer func(ydbConn *ydb.Driver) {
			errClose := ydbConn.Close(cliCtx.Context)
			if errClose != nil {
				err = fmt.Errorf("ydbConn.Close: %w: %w", errClose, err)
			}
		}(ydbConn)

		s := ysg.NewService(ydbConn)
		schemaFile, err := s.Generate(cliCtx.Context, service)
		if err != nil {
			return fmt.Errorf("s.Generate: %w", err)
		}

		err = os.WriteFile("internal/pkg/storage/schema.go", schemaFile, 0644)
		if err != nil {
			return fmt.Errorf("os.WriteFile: %w", err)
		}

		return err
	},
}
