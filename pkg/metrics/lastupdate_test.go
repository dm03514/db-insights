package metrics

import (
	"github.com/DataDog/datadog-go/statsd"
	"github.com/dm03514/db-insights/pkg/conf"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLastAccessorWorker_Emit(t *testing.T) {
	law := &LastAccessor{
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
	t.Skip()
}

func TestLastAccessorWorker_EmitAll(t *testing.T) {
	t.Skip()
}

func TestTagger_AdditionalTags(t *testing.T) {
	tests := []struct {
		Name     string
		Mappings []conf.LastUpdateTagMappings
		Input    string
		Expected []string
	}{
		{
			"no_match_false",
			[]conf.LastUpdateTagMappings{},
			"hi",
			nil,
		},
		{
			"has_match_prefix",
			[]conf.LastUpdateTagMappings{
				{
					Target:    "hi_there",
					MatchType: conf.IsPrefixMatchType,
					Tag:       "cool:tag",
				},
			},
			"hi_there_you",
			[]string{"cool:tag"},
		},
		{
			"has_match_exact",
			[]conf.LastUpdateTagMappings{
				{
					Target:    "hi_there",
					MatchType: conf.IsExactMatchType,
					Tag:       "cool:tag",
				},
			},
			"hi_there",
			[]string{"cool:tag"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			tgr, err := newTagger(tc.Mappings)
			assert.NoError(t, err)
			assert.Equal(t,
				tc.Expected,
				tgr.AdditionalTags(tc.Input),
			)
		})
	}
}
