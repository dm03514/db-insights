package metrics

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestFreshnessChecker_Emit(t *testing.T) {
	fc := &FreshnessChecker{
		Metrics: &statsd.NoOpClient{},
	}
	ok, err := fc.Emit(TableFreshness{})
	assert.NoError(t, err)
	assert.True(t, ok)
}
