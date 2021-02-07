package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dm03514/db-insights/pkg/conf"
	"github.com/dm03514/db-insights/pkg/metrics"
	log "github.com/sirupsen/logrus"
	_ "github.com/snowflakedb/gosnowflake"
)

type Snowflake struct {
	DB *sql.DB
}

func (s *Snowflake) TableAccesses(ctx context.Context, conf *conf.LastUpdateConf) ([]metrics.TableAccess, error) {
	return nil, fmt.Errorf("not supported")
}

func (s *Snowflake) Freshness(ctx context.Context, conf *conf.FreshnessConf) ([]metrics.TableFreshness, error) {
	// for each column target in conf loop through and get results
	var tfs []metrics.TableFreshness
	for _, t := range conf.Targets {
		sqlStr := fmt.Sprintf(
			"SELECT max(%s) FROM %s.%s.%s",
			t.Column,
			t.Database,
			t.Schema,
			t.Table,
		)
		log.Debugf("snowflake.Freshness: executed: %q", sqlStr)
		tf := metrics.TableFreshness{
			Database: t.Database,
			Schema:   t.Schema,
			Table:    t.Table,
			Column:   t.Column,
		}
		row := s.DB.QueryRowContext(ctx, sqlStr)
		switch err := row.Scan(&tf.LastRecord); err {
		case sql.ErrNoRows:
			return nil, fmt.Errorf("no rows were returned! %+v", t)
		case nil:
			tfs = append(tfs, tf)
		default:
			return nil, err
		}
	}

	return tfs, nil
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
