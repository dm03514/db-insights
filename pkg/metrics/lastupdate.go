package metrics

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	log "github.com/sirupsen/logrus"
	"strings"
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

type tagger struct {
	Mappings []conf.LastUpdateTagMappings
}

func newTagger(ms []conf.LastUpdateTagMappings) (tagger, error) {
	return tagger{
		Mappings: ms,
	}, nil
}

func (t tagger) AdditionalTags(s string) []string {
	var tags []string
	for _, m := range t.Mappings {
		switch m.MatchType {
		case conf.IsPrefixMatchType:
			if strings.HasPrefix(s, m.Target) {
				tags = append(tags, m.Tag)
			}
		case conf.IsExactMatchType:
			if s == m.Target {
				tags = append(tags, m.Tag)
			}
		}
	}

	return tags
}

func NewLastAccessor(ms statsd.ClientInterface, sc *conf.StaticConf, accessor Accessor) (*LastAccessor, error) {

	t, err := newTagger(sc.LastUpdates.TagMappings)
	if err != nil {
		return nil, err
	}

	return &LastAccessor{
		Metrics:    ms,
		StaticConf: sc,
		Accessor:   accessor,
		tagger:     t,
	}, nil
}

type LastAccessor struct {
	Metrics    statsd.ClientInterface
	StaticConf *conf.StaticConf
	Accessor   Accessor
	tagger     tagger
}

func (l *LastAccessor) Emit(ta TableAccess, sm map[string]struct{}) (bool, error) {
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

	/*
		additionalTags := l.tagger.AdditionalTags(ta.Table)
		if additionalTags != nil {
			for _, t := range additionalTags {
				tags = append(tags, t)
			}
		}
	*/

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

func (l *LastAccessor) EmitAll(tas []TableAccess) error {
	log.Debugf("Lastupdate received %d metrics", len(tas))
	sm := l.StaticConf.LastUpdates.SchemasMap()
	for _, ta := range tas {
		if _, err := l.Emit(ta, sm); err != nil {
			return err
		}
	}
	return nil
}

func (l *LastAccessor) QueryAccesses(ctx context.Context) error {
	// TODO - add "service" metrics to help in operating db-insights

	// do the initial collect
	tas, err := l.Accessor.TableAccesses(ctx, l.StaticConf.LastUpdates)
	if err != nil {
		return err
	}
	if err := l.EmitAll(tas); err != nil {
		return err
	}

	return nil
}
