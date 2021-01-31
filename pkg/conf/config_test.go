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
		LastUpdates: &LastUpdateConf{
			Schemas: []string{"public"},
			Since:   time.Duration(-2 * time.Hour),
		},
	}, c)
}

func TestLastUpdateConf_SchemasMap(t *testing.T) {
	t.Fail()
}
