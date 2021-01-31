package conf

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestNewFromYaml_LastUpdatesDefaults(t *testing.T) {
	c, err := NewFromYaml([]byte(``))
	assert.NoError(t, err)
	assert.Equal(t, &StaticConf{
		LastUpdates: struct {
			Schemas []string
			Since   time.Duration
		}{
			Schemas: []string{"public"},
			Since:   time.Duration(-6 * time.Hour),
		},
	}, c)
}