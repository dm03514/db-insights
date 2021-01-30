package conf

import (
	"gopkg.in/yaml.v2"
)

type StaticConf struct {
	LastUpdates struct {
		Schemas []string
	}
}

func (c *StaticConf) SetDefaults() {
	if len(c.LastUpdates.Schemas) == 0 {
		c.LastUpdates.Schemas = []string{"public"}
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
