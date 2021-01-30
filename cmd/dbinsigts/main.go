package main

import (
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	"github.com/dm03514/db-insights/pkg/redshift"
	"github.com/dm03514/db-insights/pkg/service"
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

func main() {
	fmt.Println("hi")
	app := &cli.App{
		Name: "db-insights",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "redshift-connection-string",
				Value:   "",
				Usage:   "Connection string for postgres",
				EnvVars: []string{"DB_INSIGHTS_REDSHIFT_CONN_STRING"},
			},
		},
		Action: func(c *cli.Context) error {

			ms, err := statsd.New("127.0.0.1:8125")
			ms.Tags = append(ms.Tags, "service:db-insights")
			if err != nil {
				return err
			}

			rs, err := redshift.New(c.String("redshift-connection-string"))
			if err != nil {
				log.Fatal(err)
			}
			defer rs.Close()

			if err != nil {
				return err
			}

			staticConf, err := conf.NewFromYaml([]byte(``))
			if err != nil {
				return err
			}

			serviceConf := service.Conf{
				Metrics: ms,
				DBs:     []service.DB{rs},
			}

			s, err := service.New(serviceConf, staticConf)
			if err != nil {
				return err
			}

			return s.Run()
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
