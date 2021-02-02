package redshift

import (
	"bytes"
	"context"
	"database/sql"
	"github.com/dm03514/db-insights/pkg/conf"
	"github.com/dm03514/db-insights/pkg/metrics"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"text/template"
	"time"
)

type Redshift struct {
	DB *sql.DB
}

func (r *Redshift) Close() error {
	return r.DB.Close()
}

type LastAccessContext struct {
	FromTime string
}

func (r *Redshift) TableAccesses(ctx context.Context, conf *conf.LastUpdateConf) ([]metrics.TableAccess, error) {
	now := time.Now().UTC()
	ago := now.Add(conf.Since)

	tmplContext := LastAccessContext{
		FromTime: ago.Format("2006-01-02 15:04:05"),
	}

	t, err := template.ParseFiles("pkg/redshift/sql/lastupdates.gotmpl")
	if err != nil {
	}

	var buf bytes.Buffer

	t.Execute(&buf, &tmplContext)

	log.Debugf("Executing sql: %q", buf.String())

	rows, err := r.DB.QueryContext(ctx, buf.String())
	if err != nil {
		return nil, err
	}
	var tas []metrics.TableAccess

	defer rows.Close()

	for rows.Next() {
		ta := metrics.TableAccess{}
		if err := rows.Scan(&ta.Schema, &ta.Table, &ta.LastInsert, &ta.Rows); err != nil {
			return nil, err
		}
		tas = append(tas, ta)
	}

	return tas, nil
}

func New(connStr string) (*Redshift, error) {
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

	return &Redshift{
		DB: db,
	}, nil
}
