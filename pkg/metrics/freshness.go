package metrics

import (
	"context"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	log "github.com/sirupsen/logrus"
	"time"
)

type TableFreshness struct {
	Database   string
	Schema     string
	Table      string
	LastRecord time.Time
	Column     string
	Tags       []string
}

type Freshnesser interface {
	Freshness(ctx context.Context, conf *conf.FreshnessConf) ([]TableFreshness, error)
}

type FreshnessChecker struct {
	Metrics    statsd.ClientInterface
	StaticConf *conf.StaticConf
	Fr         Freshnesser
}

func (fc *FreshnessChecker) Emit(tf TableFreshness) (bool, error) {
	tags := []string{
		fmt.Sprintf("db:%s", tf.Database),
		fmt.Sprintf("schema:%s", tf.Schema),
		fmt.Sprintf("table:%s", tf.Table),
	}
	tags = append(tags, tf.Tags...)

	diff := time.Now().UTC().Sub(tf.LastRecord)
	fc.Metrics.Gauge(
		"dbinsights.freshness.table.total_seconds",
		diff.Seconds(),
		tags,
		1,
	)
	log.Debugf(
		"freshness table: %s.%s.%s, total_seconds: %s",
		tf.Database,
		tf.Schema,
		tf.Table,
		diff,
	)
	return true, nil
}

func (fc *FreshnessChecker) EmitAll(tfs []TableFreshness) error {
	log.Debugf("EmitAll: %s", tfs)
	for _, tf := range tfs {
		if _, err := fc.Emit(tf); err != nil {
			return err
		}
	}
	return nil
}

func (fc *FreshnessChecker) Run(ctx context.Context) error {
	log.Debugf("Freshness Conf: %+v", *fc.StaticConf.Freshness)

	tf, err := fc.Fr.Freshness(ctx, fc.StaticConf.Freshness)
	if err != nil {
		return err
	}

	if err := fc.EmitAll(tf); err != nil {
		return err
	}

	return nil
}

func NewFreshnessChecker(ms statsd.ClientInterface, sc *conf.StaticConf, fr Freshnesser) (*FreshnessChecker, error) {
	return &FreshnessChecker{
		Metrics:    ms,
		StaticConf: sc,
		Fr:         fr,
	}, nil
}
