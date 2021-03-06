package conf

import (
	"gopkg.in/yaml.v2"
	"time"
)

type LastUpdateMatch string

const (
	IsPrefixMatchType LastUpdateMatch = "is_prefix"
	IsExactMatchType  LastUpdateMatch = "is_exact"
)

type LastUpdateTagMappings struct {
	Target    string
	MatchType LastUpdateMatch
	Tag       string
}

type LastUpdateConf struct {
	Schemas     []string
	Since       time.Duration // default "public"
	TagMappings []LastUpdateTagMappings
}

type TableFreshnessCheckerConf struct {
	Database string
	Schema   string
	Table    string
	Column   string
	Tags     []string
}

type FreshnessConf struct {
	Targets []TableFreshnessCheckerConf
}

func (l LastUpdateConf) SchemasMap() map[string]struct{} {
	m := make(map[string]struct{})
	for _, s := range l.Schemas {
		m[s] = struct{}{}
	}
	return m
}

type ComparisonDB struct {
	Name                   string
	Type                   string
	ConnectionStringEnvVar string `yaml:"connection_string_env_var"`
}

type ComparisonSQLStatement struct {
	DB   string
	SQL  string
	Name string
}

type ComparisonTarget struct {
	Name          string
	SQLStatements []ComparisonSQLStatement `yaml:"sql_statements"`
}

type ComparisonsConf struct {
	DBs     []ComparisonDB
	Targets []ComparisonTarget
}

type StaticConf struct {
	LastUpdates *LastUpdateConf
	Freshness   *FreshnessConf
	Comparisons *ComparisonsConf
}

func (c *StaticConf) SetDefaults() {
	if c.LastUpdates == nil {
		c.LastUpdates = &LastUpdateConf{}
	}

	if len(c.LastUpdates.Schemas) == 0 {
		c.LastUpdates.Schemas = []string{"public"}
	}

	if c.LastUpdates.Since.Seconds() == 0 {
		c.LastUpdates.Since = time.Duration(-2 * time.Hour)
	}
}

func NewFromYaml(data []byte) (*StaticConf, error) {
	var c StaticConf
	if err := yaml.Unmarshal([]byte(data), &c); err != nil {
		return nil, err
	}

	c.SetDefaults()

	return &c, nil
}
