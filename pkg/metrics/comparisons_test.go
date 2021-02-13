package metrics

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func Test_envContext_NoDBIValues(t *testing.T) {
	out := envContext([]string{"NOPE=miss", "notheregoodfriend=miss"})
	assert.Equal(t, map[string]string{}, out)
}

func Test_envContext_WithDBIValues(t *testing.T) {
	out := envContext([]string{
		"DBIhi=contextval",
		"notheregoodfriend=miss",
		"dbistrictcasing=miss",
	})
	assert.Equal(t, map[string]string{
		"DBIhi": "contextval",
	}, out)
}

func Test_sqlFromTemplateString_noSubstititions(t *testing.T) {
	template := "hi i'm a really cool sql template!"
	sc := SQLContext{}
	sql, err := sqlFromTemplateString(template, sc)
	assert.NoError(t, err)
	assert.Equal(t, template, sql)
}

func Test_sqlFromTemplateString_WithSubstition(t *testing.T) {
	template := "hi i'm a really cool sql template! '{{ .Env.DBIHi }}'"
	sc := SQLContext{
		Env: map[string]string{
			"DBIHi": "rendered 2020-01-10",
		},
	}
	sql, err := sqlFromTemplateString(template, sc)
	assert.NoError(t, err)
	assert.Equal(t,
		"hi i'm a really cool sql template! 'rendered 2020-01-10'",
		sql,
	)
}

func TestComparisonResult_TargetName(t *testing.T) {
	cr := ComparisonResult{
		Name: "hi",
		First: Result{
			Key:  "hi",
			DB:   "first_db",
			Name: "first_target",
		},
		Second: Result{
			DB:   "second_db",
			Name: "second_target",
		},
	}

	assert.Equal(t, "hi_first_db_first_target_second_db_second_target", cr.TargetName())
}

func TestComparisonResult_RatioFirstToSecond(t *testing.T) {
	tests := []struct {
		FirstRows  int
		SecondRows int
		Expected   float64
	}{
		{
			1,
			0,
			math.Inf(1),
		},
		{
			0,
			1,
			0,
		},
		{
			100,
			50,
			2,
		},
		{
			50,
			100,
			0.5,
		},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("first_%d_second_%d", tc.FirstRows, tc.SecondRows), func(t *testing.T) {
			cr := ComparisonResult{
				First: Result{
					Rows: tc.FirstRows,
				},
				Second: Result{
					Rows: tc.SecondRows,
				},
			}
			assert.Equal(t, tc.Expected, cr.RatioFirstToSecond())
		})
	}
}
