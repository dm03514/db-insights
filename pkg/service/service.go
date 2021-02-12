package service

import (
	"context"
	"database/sql"
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
	metrics.Freshnesser
	metrics.Accessor

	Close() error
	SQLDB() *sql.DB
}

type Conf struct {
	Metrics statsd.ClientInterface
	DB      DB
}

type Service struct {
	Conf       Conf
	StaticConf *conf.StaticConf
}

func (s *Service) Close() error {
	s.Conf.Metrics.Flush()
	return s.Conf.DB.Close()
}

func (s *Service) CheckComparisons() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := metrics.NewCompareChecker(
		s.Conf.Metrics,
		s.StaticConf,
		s.Conf.DB.SQLDB(),
	)
	if err != nil {
		return err
	}

	return f.Run(ctx)
}

func (s *Service) CheckFreshness() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f, err := metrics.NewFreshnessChecker(
		s.Conf.Metrics,
		s.StaticConf,
		s.Conf.DB,
	)
	if err != nil {
		return err
	}

	return f.Run(ctx)
}

func (s *Service) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Assuming a single db for right now

	accessWorker, err := metrics.NewLastAccessor(
		s.Conf.Metrics,
		s.StaticConf,
		s.Conf.DB,
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
