package main

import (
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	"github.com/dm03514/db-insights/pkg/service"
	"github.com/dm03514/db-insights/pkg/snowflake"
	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
	"os"
)

const envLogLevel string = "DB_INSIGHTS_LOG_LEVEL"

func init() {
	// Log as JSON instead of the default ASCII formatter.
	// log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	switch os.Getenv(envLogLevel) {
	case "INFO":
		log.SetLevel(log.InfoLevel)
	case "WARN":
		log.SetLevel(log.WarnLevel)
	default:
		log.SetLevel(log.DebugLevel)
	}
}

func newService(c *cli.Context) (*service.Service, error) {
	ms, err := statsd.New("127.0.0.1:8125")
	ms.Tags = append(ms.Tags, "service:db-insights")
	if err != nil {
		return nil, err
	}

	var db service.DB
	err = nil
	switch c.String("db") {
	case "snowflake":
		db, err = snowflake.New(c.String("connection-string"))
	default:
		return nil, fmt.Errorf(
			"db: %q, not supported. Currently only 'snowflake' is suported.",
			c.String("db"),
		)
	}

	if err != nil {
		return nil, err
	}

	staticConf, err := conf.NewFromYaml([]byte(``))
	if err != nil {
		return nil, err
	}

	serviceConf := service.Conf{
		Metrics: ms,
		DB:      db,
	}

	s, err := service.New(serviceConf, staticConf)
	if err != nil {
		return nil, err
	}

	return s, nil
}

func main() {
	fmt.Println("hi")
	app := &cli.App{
		Name: "db-insights",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "connection-string",
				Value:       "",
				Usage:       "Connection string",
				DefaultText: "test",
				EnvVars:     []string{"DB_INSIGHTS_CONN_STRING"},
			},
			&cli.StringFlag{
				Name:  "db",
				Value: "snowflake",
				Usage: "The concrete database to use: snowflake",
			},
		},
		Commands: []*cli.Command{
			/*
				{
					Name: "service",
					Action: func(c *cli.Context) error {
						s, err := newService(c)
						if err != nil {
							return err
						}
						defer s.Close()
						return s.Run()
					},
				},
			*/
			{
				Name:  "check",
				Usage: "execute a single check using the CLI",
				Subcommands: []*cli.Command{
					{
						Name:  "freshness",
						Usage: "check freshness",
						Action: func(c *cli.Context) error {
							s, err := newService(c)
							if err != nil {
								return err
							}
							defer s.Close()
							return s.CheckFreshness()
						},
					},
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
