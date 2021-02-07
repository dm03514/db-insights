package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dm03514/db-insights/pkg/conf"
	"github.com/dm03514/db-insights/pkg/metrics"
	_ "github.com/snowflakedb/gosnowflake"
)

type Snowflake struct {
	DB *sql.DB
}

func (s *Snowflake) TableAccesses(ctx context.Context, conf *conf.LastUpdateConf) ([]metrics.TableAccess, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *Snowflake) Freshness(ctx context.Context, conf *conf.FreshnessConf) ([]metrics.TableFreshness, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *Snowflake) Close() error {
	return s.DB.Close()
}

func New(connStr string) (*Snowflake, error) {
	db, err := sql.Open(
		"snowflake",
		connStr,
	)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	return &Snowflake{
		DB: db,
	}, nil
}
