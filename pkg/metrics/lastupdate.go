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

type tagger struct{}

func (t tagger) AdditionalTags(ta TableAccess) []string {
	return nil
}

func NewLastAccessWorker(ms statsd.ClientInterface, cd time.Duration, sc *conf.StaticConf, accessor Accessor) (*LastAccessorWorker, error) {
	t := tagger{}

	return &LastAccessorWorker{
		Metrics:            ms,
		CollectionDuration: cd,
		StaticConf:         sc,
		Accessor:           accessor,
		tagger:             t,
	}, nil
}

type LastAccessorWorker struct {
	Metrics            statsd.ClientInterface
	CollectionDuration time.Duration
	StaticConf         *conf.StaticConf
	Accessor           Accessor
	tagger             tagger
}

func (l *LastAccessorWorker) Emit(ta TableAccess, sm map[string]struct{}) (bool, error) {
	// check if the table access matches
	// this will eventually become a query predicate.
	if _, ok := sm[ta.Schema]; !ok {
		return false, nil
	}

	// if it does match calculate any additional tags
	tags := []string{
		fmt.Sprintf("schema:%s", ta.Schema),
		fmt.Sprintf("table:%s", ta.Table),
	}

	additionalTags := l.tagger.AdditionalTags(ta)
	if additionalTags != nil {
		for _, t := range additionalTags {
			tags = append(tags, t)
		}
	}

	diff := time.Now().UTC().Sub(ta.LastInsert)

	l.Metrics.Histogram(
		"dbinsights.lastupdater.table.total_seconds",
		diff.Seconds(),
		tags,
		1,
	)
	l.Metrics.Histogram(
		"dbinsights.lastupdater.table.rows",
		float64(ta.Rows),
		tags,
		1,
	)

	log.Debugf("lastupdater table: %q.%q, rows: %d, duration: %s",
		ta.Schema, ta.Table, ta.Rows, diff)

	return true, nil
}

func (l *LastAccessorWorker) EmitAll(tas []TableAccess) error {
	log.Debugf("Lastupdate received %d metrics", len(tas))
	sm := l.StaticConf.LastUpdates.SchemasMap()
	for _, ta := range tas {
		if _, err := l.Emit(ta, sm); err != nil {
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
			log.Debugf("lastupdater.Loop ctx.Done()")
			return
		case <-ticker.C:
			log.Debugf("lastupdater.Loop getting accesses")
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
