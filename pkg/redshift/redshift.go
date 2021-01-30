package redshift

import (
	"context"
	"database/sql"
	"github.com/dm03514/db-insights/pkg/metrics"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"time"
)

type Redshift struct {
	DB *sql.DB
}

func (r *Redshift) Close() error {
	return r.DB.Close()
}

func (r *Redshift) TableAccesses(ctx context.Context, schemas []string) ([]metrics.TableAccess, error) {
	now := time.Now()
	ago := now.Add(time.Duration(-3) * time.Hour)
	fromTime := ago.Format("2006-01-02 15:04:05")

	// pwd, _ := os.Getwd()
	bs, err := ioutil.ReadFile( "pkg/redshift/sql/lastupdates.sql")
	// t, err := template.ParseFiles("pkg/redshift/sql/lastupdates.sql")
	if err != nil {
		return nil, err
	}

	log.Debugf("Executing sql: %q", string(bs))

	rows, err := r.DB.QueryContext(ctx, string(bs), fromTime, schemas)
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
