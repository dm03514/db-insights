package metrics

import (
	"context"
	"fmt"
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
	TableAccesses(ctx context.Context, conf *conf.LastUpdateConf) ([]TableAccess, error)
}

type LastAccessorWorker struct {
	Metrics            statsd.ClientInterface
	CollectionDuration time.Duration
	StaticConf         *conf.StaticConf
	Accessor           Accessor
}

func (l *LastAccessorWorker) Emit(ta TableAccess) (bool, error) {
	// check if the table access matches

	// if it does match calculate any additional tags

	tags := []string{
		fmt.Sprintf("schema:%s", ta.Schema),
		fmt.Sprintf("table:%s", ta.Table),
	}

	diff := time.Now().UTC().Sub(time.Now().UTC())

	l.Metrics.Histogram(
		"dbinsights.lastupdates.table.total_seconds",
		diff.Seconds(),
		tags,
		1,
	)
	l.Metrics.Histogram(
		"dbinsights.lastupdates.table.rows",
		float64(ta.Rows),
		tags,
		1,
	)

	return true, nil
}

func (l *LastAccessorWorker) EmitAll(tas []TableAccess) error {
	log.Debugf("Lastupdate received %d metrics", len(tas))
	for _, ta := range tas {
		if _, err := l.Emit(ta); err != nil {
			return err
		}
	}
	return nil
}

func (l *LastAccessorWorker) Loop(ctx context.Context) {
	ticker := time.NewTicker(l.CollectionDuration)
	defer ticker.Stop()

	// do the initial collect
	tas, err := l.Accessor.TableAccesses(ctx, l.StaticConf.LastUpdates)
	if err != nil {
		panic(err)
	}
	if err := l.EmitAll(tas); err != nil {
		panic(err)
	}

	for {
		select {
		case <-ctx.Done():
			log.Debugf("lastupdate.Loop ctx.Done()")
			return
		case <-ticker.C:
			log.Debugf("lastupdate.Loop getting accesses")
			tas, err := l.Accessor.TableAccesses(ctx, l.StaticConf.LastUpdates)
			if err != nil {
				panic(err)
			}
			if err := l.EmitAll(tas); err != nil {
				panic(err)
			}
		}
	}
}
