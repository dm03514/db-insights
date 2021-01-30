package conf

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewFromYaml_LastUpdatesDefaults(t *testing.T) {
	c, err := NewFromYaml([]byte(``))
	assert.NoError(t, err)
	assert.Equal(t, &StaticConf{
		LastUpdates: struct {
			Schemas []string
		}{
			Schemas: []string{"public"},
		},
	}, c)
}
