package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dm03514/db-insights/pkg/conf"
	"github.com/dm03514/db-insights/pkg/metrics"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
)

type Postgres struct {
	DB *sql.DB
}

func (p *Postgres) SQLDB() *sql.DB {
	return p.DB
}

func (p *Postgres) Close() error {
	return p.DB.Close()
}

func (p *Postgres) TableAccesses(ctx context.Context, conf *conf.LastUpdateConf) ([]metrics.TableAccess, error) {
	return nil, fmt.Errorf("not supported")
}

func (p *Postgres) Compare(ctx context.Context, conf *conf.ComparisonsConf) ([]metrics.Comparison, error) {
	return nil, fmt.Errorf("not supported")
}

func (p *Postgres) Freshness(ctx context.Context, conf *conf.FreshnessConf) ([]metrics.TableFreshness, error) {
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
		log.Debugf("postgres.Freshness: executed: %q", sqlStr)
		tf := metrics.TableFreshness{
			Database: t.Database,
			Schema:   t.Schema,
			Table:    t.Table,
			Column:   t.Column,
		}
		row := p.DB.QueryRowContext(ctx, sqlStr)
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

func New(connStr string) (*Postgres, error) {
	db, err := sql.Open(
		"postgres",
		connStr,
	)
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		return nil, err
	}

	return &Postgres{
		DB: db,
	}, nil
}
