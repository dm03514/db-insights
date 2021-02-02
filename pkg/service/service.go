package service

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	"github.com/dm03514/db-insights/pkg/metrics"
	"github.com/robfig/cron/v3"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
)

type DB interface {
	TableAccesses(ctx context.Context, conf *conf.LastUpdateConf) ([]metrics.TableAccess, error)
}

type Conf struct {
	Metrics statsd.ClientInterface
	DBs     []DB
}

type Service struct {
	Conf       Conf
	StaticConf *conf.StaticConf
}

func (s *Service) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Assuming a single db for right now

	accessWorker, err := metrics.NewLastAccessor(
		s.Conf.Metrics,
		s.StaticConf,
		s.Conf.DBs[0],
	)
	if err != nil {
		return err
	}

	c := cron.New()
	c.AddFunc("@hourly", func() {
		if err := accessWorker.QueryAccesses(ctx); err != nil {
			log.Error(err)
		}
	})

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	// Block until we receive our signal.
	<-interruptChan

	log.Println("Shutting down")
	return nil
}

func New(c Conf, sc *conf.StaticConf) (*Service, error) {
	return &Service{
		Conf:       c,
		StaticConf: sc,
	}, nil
}
