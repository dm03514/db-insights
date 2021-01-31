package metrics

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLastAccessorWorker_Emit(t *testing.T) {
	law := &LastAccessorWorker{
		Metrics: &statsd.NoOpClient{},
	}
	ok, err := law.Emit(TableAccess{})
	assert.NoError(t, err)
	assert.True(t, ok)
}
