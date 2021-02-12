package metrics

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"os"
)

type ComparisonResult struct {
	Name string
	SQL  string
	DB   string
	Rows int
}

type Comparison struct {
	Results []ComparisonResult
}

type CompareChecker struct {
	Metrics    statsd.ClientInterface
	StaticConf *conf.StaticConf

	dbs map[string]*sql.DB
}

func (cc *CompareChecker) Run(ctx context.Context) error {
	log.Debugf("CompareConf: %+v", *cc.StaticConf.Comparisons)

	for _, comps := range cc.StaticConf.Comparisons.Targets {
		log.Debugf("%+v", comps)
	}

	return nil
}

// NewCompareChecker builds a compare checker and initializes all
// underlying resources.
func NewCompareChecker(ms statsd.ClientInterface, sc *conf.StaticConf, primaryDB *sql.DB) (*CompareChecker, error) {
	// initialize dbs
	if len(sc.Comparisons.DBs) == 0 {
		return nil, fmt.Errorf("must include an addditional db")
	}
	cc := &CompareChecker{
		Metrics:    ms,
		StaticConf: sc,

		dbs: make(map[string]*sql.DB),
	}

	cc.dbs["primary"] = primaryDB

	for _, db := range sc.Comparisons.DBs {
		switch db.Type {
		case "redshift":
			// get connection string
			conn, err := sql.Open(
				"postgres",
				os.Getenv(db.ConnectionStringEnvVar),
			)

			if err = conn.Ping(); err != nil {
				return nil, err
			}

			if err != nil {
				return nil, err
			}

			cc.dbs[db.Name] = conn

		default:
			return nil, fmt.Errorf("db: %q not supported", db.Type)
		}
	}

	return cc, nil
}
