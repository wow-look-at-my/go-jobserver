package jobserver

import _ "embed"

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/good-defs.xml
var goodDefs string

func TestParseMonitorDefs(t *testing.T) {
	defs, err := ParseMonitorDefs([]byte(goodDefs))
	require.NoError(t, err)
	names := []string{}
	for _, k := range defs.Kinds() {
		names = append(names, k.Name)
	}
	assert.Equal(t, []string{"file-exists", "line", "status-ok"}, names)

	k, ok := defs.Kind("status-ok")
	require.True(t, ok)
	assert.Equal(t, "poll", k.Shape)
	assert.Equal(t, "request", k.Source)
	assert.Equal(t, 2*time.Second, k.Every)
	assert.Equal(t, time.Second, k.Timeout)
	assert.Equal(t, "GET", k.request.method)

	k, _ = defs.Kind("file-exists")
	assert.Equal(t, "argv", k.Source)
	assert.Equal(t, DefaultMonitorTimeout, k.Timeout)
	k, _ = defs.Kind("line")
	assert.Equal(t, "stream", k.Shape)
	assert.Equal(t, 3*time.Second, k.Every)
}

// Each rule names itself in the error, so a broken definition says what to fix.
func TestParseMonitorDefsRejects(t *testing.T) {
	cases := map[string]struct{ doc, want string }{
		"wrong root":          {`<monitor name="x"/>`, "want <monitors>"},
		"unknown element":     {`<monitors><job/></monitors>`, "unknown element <job>"},
		"unknown attribute":   {`<monitors><monitor name="x" every="1s"><poll><argv>true</argv></poll></monitor></monitors>`, `unknown attribute "every"`},
		"no name":             {`<monitors><monitor><poll><argv>true</argv></poll></monitor></monitors>`, "needs name="},
		"twice":               {`<monitors><monitor name="x"><poll><argv>true</argv></poll></monitor><monitor name="x"><poll><argv>true</argv></poll></monitor></monitors>`, "defined twice"},
		"no check":            {`<monitors><monitor name="x"/></monitors>`, "needs a <poll> or a <stream>"},
		"two checks":          {`<monitors><monitor name="x"><poll><argv>a</argv></poll><stream><argv>b</argv></stream><until>x</until></monitor></monitors>`, "a monitor has one check"},
		"shell string":        {`<monitors><monitor name="x"><poll>test -e /tmp/x</poll></monitor></monitors>`, "never a shell string"},
		"request and argv":    {`<monitors><monitor name="x"><poll><request><url>u</url></request><argv>a</argv></poll><until>x</until></monitor></monitors>`, "one or the other"},
		"two requests":        {`<monitors><monitor name="x"><poll><request><url>u</url></request><request><url>v</url></request></poll><until>x</until></monitor></monitors>`, "not two requests"},
		"stream request":      {`<monitors><monitor name="x"><stream><request><url>u</url></request></stream><until>x</until></monitor></monitors>`, "holds only <argv>"},
		"empty poll":          {`<monitors><monitor name="x"><poll/></monitor></monitors>`, "is empty"},
		"request no until":    {`<monitors><monitor name="x"><poll><request><url>u</url></request></poll></monitor></monitors>`, "needs <until>"},
		"stream no until":     {`<monitors><monitor name="x"><stream><argv>a</argv></stream></monitor></monitors>`, "needs <until>"},
		"request no url":      {`<monitors><monitor name="x"><poll><request/></poll><until>x</until></monitor></monitors>`, "needs <url>"},
		"cadence below floor": {`<monitors><monitor name="x"><poll every="10ms"><argv>a</argv></poll></monitor></monitors>`, "below the"},
		"bad duration":        {`<monitors><monitor name="x"><poll every="soon"><argv>a</argv></poll></monitor></monitors>`, `every="soon"`},
		"arg twice":           {`<monitors><monitor name="x"><arg name="a"/><arg name="a"/><poll><argv>a</argv></poll></monitor></monitors>`, "declared twice"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ParseMonitorDefs([]byte(c.doc))
			require.Error(t, err)
			assert.Contains(t, err.Error(), c.want)
		})
	}
}

func TestMonitorDefsValidate(t *testing.T) {
	defs, err := ParseMonitorDefs([]byte(goodDefs))
	require.NoError(t, err)
	require.NoError(t, defs.Validate(MonitorSpec{Kind: "status-ok", Args: map[string]string{"svc": "db"}}))
	assert.ErrorIs(t, defs.Validate(MonitorSpec{Kind: "nope"}), ErrUnknownMonitorKind)
	assert.ErrorIs(t, defs.Validate(MonitorSpec{Kind: "status-ok"}), ErrBadMonitorArgs, "a required arg is missing")
	assert.ErrorIs(t, defs.Validate(MonitorSpec{Kind: "status-ok", Args: map[string]string{"svc": "db", "x": "1"}}), ErrBadMonitorArgs, "an undeclared arg")
	var none *MonitorDefs
	assert.ErrorIs(t, none.Validate(MonitorSpec{Kind: "status-ok"}), ErrUnknownMonitorKind, "a server with no definitions knows no kind")
}

func TestMonitorSpecKey(t *testing.T) {
	a := MonitorSpec{Kind: "k", Args: map[string]string{"b": "2", "a": "1"}}
	b := MonitorSpec{Kind: "k", Args: map[string]string{"a": "1", "b": "2"}}
	assert.Equal(t, "monitor:k a=1 b=2", a.Key())
	assert.Equal(t, a.Key(), b.Key(), "arg order does not change the key")
}
