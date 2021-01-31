package service

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	"github.com/dm03514/db-insights/pkg/metrics"
	log "github.com/sirupsen/logrus"
	"os"
	"os/signal"
	"syscall"
	"time"
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

	accessWorker := metrics.LastAccessorWorker{
		Metrics:            s.Conf.Metrics,
		CollectionDuration: 1 * time.Hour,
		Accessor:           s.Conf.DBs[0],
		StaticConf:         s.StaticConf,
	}

	go accessWorker.Loop(ctx)

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
