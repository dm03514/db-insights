package conf

import (
	"gopkg.in/yaml.v2"
	"time"
)

type StaticConf struct {
	LastUpdates struct {
		Schemas []string
		Since   time.Duration
	}
}

func (c *StaticConf) SetDefaults() {
	if len(c.LastUpdates.Schemas) == 0 {
		c.LastUpdates.Schemas = []string{"public"}
	}

	if c.LastUpdates.Since.Seconds() == 0 {
		c.LastUpdates.Since = time.Duration(-6 * time.Hour)
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
