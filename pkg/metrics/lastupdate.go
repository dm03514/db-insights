package metrics

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	log "github.com/sirupsen/logrus"
	"time"
)

type TableAccess struct {
	Schema     string
	Table      string
	LastInsert time.Time
	Rows       int
}

type Accessor interface {
	// In the future this should be a cursor or generator
	// pull everything into memory for right now for simplicity
	TableAccesses(ctx context.Context, schemas []string) ([]TableAccess, error)
}

type LastAccessorWorker struct {
	Metrics            statsd.ClientInterface
	CollectionDuration time.Duration
	StaticConf         *conf.StaticConf
	Accessor           Accessor
}

func (l *LastAccessorWorker) Emit(tas []TableAccess) error {
	log.Debug(tas)
	return nil
}

func (l *LastAccessorWorker) Loop(ctx context.Context) {
	ticker := time.NewTicker(l.CollectionDuration)
	defer ticker.Stop()

	// do the initial collect
	tas, err := l.Accessor.TableAccesses(ctx, l.StaticConf.LastUpdates.Schemas)
	if err != nil {
		panic(err)
	}
	if err := l.Emit(tas); err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Debugf("lastupdate.Loop ctx.Done()")
			return
		case <-ticker.C:
			log.Debugf("lastupdate.Loop getting accesses")
			tas, err := l.Accessor.TableAccesses(ctx, l.StaticConf.LastUpdates.Schemas)
			if err != nil {
				panic(err)
			}
			if err := l.Emit(tas); err != nil {
				panic(err)
			}
		}
	}
}
