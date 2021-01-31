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
	schemas := map[string]struct{}{
		"public": {},
	}
	ok, err := law.Emit(TableAccess{
		Schema: "public",
	}, schemas)
	assert.NoError(t, err)
	assert.True(t, ok)
}

func TestLastAccessorWorker_Emit_Additional_Tags(t *testing.T) {
	t.Fail()
}

func TestLastAccessorWorker_EmitAll(t *testing.T) {
	t.Fail()
}

func TestTagger_AdditionalTags(t *testing.T) {
	t.Fail()
}
