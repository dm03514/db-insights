package metrics

import (
	"context"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
)

type TableFreshness struct{}

type Freshnesser interface {
	Freshness(ctx context.Context, conf *conf.FreshnessConf) ([]TableFreshness, error)
}

type FreshnessChecker struct {
	Metrics    statsd.ClientInterface
	StaticConf *conf.StaticConf
	Fr         Freshnesser
}

func (fc *FreshnessChecker) Run(ctx context.Context) error {
	return nil
}

func NewFreshnessChecker(ms statsd.ClientInterface, sc *conf.StaticConf, fr Freshnesser) (*FreshnessChecker, error) {
	return &FreshnessChecker{
		Metrics:    ms,
		StaticConf: sc,
		Fr:         fr,
	}, nil
}
