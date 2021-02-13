package metrics

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	_ "github.com/lib/pq"
	log "github.com/sirupsen/logrus"
	"html/template"
	"os"
	"strings"
)

type Result struct {
	Key  string
	Name string
	DB   string
	Rows int
}

type ComparisonResult struct {
	Name   string
	First  Result
	Second Result
}

func (cr ComparisonResult) Key() string {
	return cr.First.Key
}

func (cr ComparisonResult) TargetName() string {
	return fmt.Sprintf("%s_%s_%s_%s_%s",
		cr.Key(),
		cr.First.DB,
		cr.First.Name,
		cr.Second.DB,
		cr.Second.Name,
	)
}

func (cr ComparisonResult) RatioFirstToSecond() float64 {
	return float64(cr.First.Rows) / float64(cr.Second.Rows)
}

type Comparison struct {
	Results []ComparisonResult
}

type CompareChecker struct {
	Metrics    statsd.ClientInterface
	StaticConf *conf.StaticConf

	dbs map[string]*sql.DB
}

func envContext(env []string) map[string]string {
	ec := make(map[string]string)
	for _, e := range env {
		pair := strings.SplitN(e, "=", 2)
		key, val := pair[0], pair[1]
		if strings.HasPrefix(key, "DBI") {
			ec[key] = val
		}
	}
	return ec
}

type SQLContext struct {
	Env map[string]string
}

func sqlFromTemplateString(t string, sc SQLContext) (string, error) {
	tmpl, err := template.New("sql").Parse(t)
	if err != nil {
		return "", err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, sc)

	return buf.String(), err
}

func (cc *CompareChecker) ExecuteComp(ctx context.Context, comp conf.ComparisonSQLStatement) (map[string]Result, error) {
	// execute the sql against the db
	db, ok := cc.dbs[comp.DB]
	if !ok {
		return nil, fmt.Errorf("db %q not registered", comp.DB)
	}

	sc := SQLContext{
		Env: envContext(os.Environ()),
	}
	// sql is a template build the template
	sql, err := sqlFromTemplateString(comp.SQL, sc)
	if err != nil {
		return nil, err
	}

	log.Debugf("Compare Conf db: %q, Executing SQL: %q",
		comp.Name,
		sql,
	)

	rows, err := db.QueryContext(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// the results may contain multiple records we need to unpack those into a map
	results := make(map[string]Result)

	for rows.Next() {
		r := Result{
			DB:   comp.DB,
			Name: comp.Name,
		}
		if err := rows.Scan(&r.Key, &r.Rows); err != nil {
			return nil, err
		}
		results[r.Key] = r
	}

	return results, nil
}

func (cc *CompareChecker) Emit(cr ComparisonResult) error {
	// emit the target comparision
	log.Infof(
		"dbinsights.comparisons.ratio, value: %f, tags: %+v",
		cr.RatioFirstToSecond(),
		[]string{
			fmt.Sprintf("key:%s", cr.Key()),
			fmt.Sprintf("comp_name:%s", cr.Name),
			fmt.Sprintf("target_name:%s", cr.TargetName()),
		},
	)

	cc.Metrics.Gauge(
		"dbinsights.comparisons.ratio",
		cr.RatioFirstToSecond(),
		[]string{
			fmt.Sprintf("key:%s", cr.Key()),
			fmt.Sprintf("comp_name:%s", cr.Name),
			fmt.Sprintf("target_name:%s", cr.TargetName()),
		},
		1,
	)

	log.Infof(
		"dbinsights.comparisons.single_target, value: %f, tags: %+v",
		float64(cr.First.Rows),
		[]string{
			fmt.Sprintf("key:%s", cr.Key()),
			fmt.Sprintf("comp_name:%s", cr.Name),
			fmt.Sprintf("db:%s", cr.First.DB),
			fmt.Sprintf("name:%s", cr.First.Name),
		},
	)

	// emit the first rows by itself
	cc.Metrics.Gauge(
		"dbinsights.comparisons.single_target",
		float64(cr.First.Rows),
		[]string{
			fmt.Sprintf("key:%s", cr.Key()),
			fmt.Sprintf("comp_name:%s", cr.Name),
			fmt.Sprintf("db:%s", cr.First.DB),
			fmt.Sprintf("name:%s", cr.First.Name),
		},
		1,
	)

	log.Infof(
		"dbinsights.comparisons.single_target, value: %f, tags: %+v",
		float64(cr.Second.Rows),
		[]string{
			fmt.Sprintf("key:%s", cr.Key()),
			fmt.Sprintf("comp_name:%s", cr.Name),
			fmt.Sprintf("db:%s", cr.Second.DB),
			fmt.Sprintf("name:%s", cr.Second.Name),
		},
	)

	// emit the second rows by itself
	cc.Metrics.Gauge(
		"dbinsights.comparisons.single_target",
		float64(cr.Second.Rows),
		[]string{
			fmt.Sprintf("key:%s", cr.Key()),
			fmt.Sprintf("comp_name:%s", cr.Name),
			fmt.Sprintf("db:%s", cr.Second.DB),
			fmt.Sprintf("name:%s", cr.Second.Name),
		},
		1,
	)

	return nil
}

func (cc *CompareChecker) Run(ctx context.Context) error {
	log.Debugf("CompareConf: %+v", *cc.StaticConf.Comparisons)

	for _, comps := range cc.StaticConf.Comparisons.Targets {
		// statically limit to 2 statements
		if len(comps.SQLStatements) != 2 {
			return fmt.Errorf("only supports 2 comparisons right now")
		}
		first, err := cc.ExecuteComp(ctx, comps.SQLStatements[0])
		log.Debugf("conn: %q, results: %+v", comps.SQLStatements[0].DB, first)
		if err != nil {
			return err
		}
		second, err := cc.ExecuteComp(ctx, comps.SQLStatements[1])
		log.Debugf("conn: %q, results: %+v", comps.SQLStatements[1].DB, second)
		if err != nil {
			return err
		}
		if len(first) != len(second) {
			return fmt.Errorf("check that sql statements return the same number of records")
		}

		var crs []ComparisonResult
		for k, val1 := range first {
			val2, ok := second[k]
			if !ok {
				return fmt.Errorf("key: %q not present in %+v", k, second)
			}
			cr := ComparisonResult{
				Name:   comps.Name,
				First:  val1,
				Second: val2,
			}
			crs = append(crs, cr)
		}

		for _, cr := range crs {
			if err := cc.Emit(cr); err != nil {
				return err
			}
		}

		log.Debugf("%+v", crs)
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
