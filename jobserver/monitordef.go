// Monitor definitions: an XML document read with api-dsl.

package jobserver

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	apidsl "github.com/wow-look-at-my/api-dsl"
	"github.com/wow-look-at-my/go-containers/set"
)

// Monitor definitions: an XML document read with api-dsl. Each <monitor> is one kind of condition a monitor job checks.

// Defaults and floors for a monitor's cadence.
const (
	DefaultMonitorEvery   = 30 * time.Second
	DefaultMonitorTimeout = 20 * time.Second
	minMonitorEvery       = time.Second
)

// ErrUnknownMonitorKind is returned for a monitor spec that names no definition.
var ErrUnknownMonitorKind = errors.New("jobserver: no such monitor kind")

// ErrBadMonitorArgs is returned for a monitor spec whose args do not match its definition.
var ErrBadMonitorArgs = errors.New("jobserver: bad monitor args")

// MonitorArg is one argument a monitor kind declares.
type MonitorArg struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

// MonitorKind is one <monitor> definition.
type MonitorKind struct {
	Name        string       `json:"name"`
	Description string       `json:"description,omitempty"`
	Args        []MonitorArg `json:"args,omitempty"`
	// Shape is "poll" or "stream".
	Shape string `json:"shape"`
	// Every is the poll cadence, or the restart delay of a stream.
	Every time.Duration `json:"every"`
	// Timeout bounds one poll check.
	Timeout time.Duration `json:"timeout,omitempty"`
	// Source is "request" or "argv".
	Source string `json:"source"`

	request  *monitorRequest
	argv     []string
	until    string
	describe string
}

// monitorRequest is a compiled <request>. Every string is a template.
type monitorRequest struct {
	method    string
	url       string
	headers   [][2]string
	body      string
	transport string
}

// MonitorDefs is a parsed definitions document.
type MonitorDefs struct {
	kinds map[string]*MonitorKind
	vars  map[string]any
}

// LoadMonitorDefs reads and parses a definitions file.
func LoadMonitorDefs(path string) (*MonitorDefs, error) {
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	defs, err := ParseMonitorDefs(src)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return defs, nil
}

// ParseMonitorDefs parses a <monitors> document. An element or attribute it does not know is an error.
func ParseMonitorDefs(src []byte) (*MonitorDefs, error) {
	root, err := apidsl.ParseDOM(src)
	if err != nil {
		return nil, err
	}
	if root.Name() != "monitors" {
		return nil, fmt.Errorf("root element is <%s>, want <monitors>", root.Name())
	}
	if err := apidsl.CheckAttrs(root); err != nil {
		return nil, err
	}
	defs := &MonitorDefs{kinds: map[string]*MonitorKind{}, vars: map[string]any{}}
	for _, n := range root.Children() {
		switch n.Name() {
		case "vars":
			if err := defs.parseVars(n); err != nil {
				return nil, err
			}
		case "monitor":
			k, err := parseMonitor(n)
			if err != nil {
				return nil, err
			}
			if _, dup := defs.kinds[k.Name]; dup {
				return nil, fmt.Errorf("<monitor name=%q> is defined twice", k.Name)
			}
			defs.kinds[k.Name] = k
		default:
			return nil, fmt.Errorf("<monitors>: unknown element <%s>", n.Name())
		}
	}
	return defs, nil
}

func (d *MonitorDefs) parseVars(n *apidsl.Node) error {
	if err := apidsl.CheckAttrs(n); err != nil {
		return err
	}
	for _, v := range n.Children() {
		if v.Name() != "var" {
			return fmt.Errorf("<vars>: unknown element <%s>", v.Name())
		}
		if err := apidsl.CheckAttrs(v, "name"); err != nil {
			return err
		}
		name := v.Attr("name")
		if name == "" {
			return errors.New("<var> needs name=")
		}
		text, err := apidsl.TextOf(v)
		if err != nil {
			return fmt.Errorf("<var name=%q>: %w", name, err)
		}
		d.vars[name] = strings.TrimSpace(text)
	}
	return nil
}

func parseMonitor(n *apidsl.Node) (*MonitorKind, error) {
	if err := apidsl.CheckAttrs(n, "name", "description"); err != nil {
		return nil, err
	}
	k := &MonitorKind{Name: n.Attr("name"), Description: n.Attr("description")}
	if k.Name == "" {
		return nil, errors.New("<monitor> needs name=")
	}
	fail := func(format string, args ...any) error {
		return fmt.Errorf("<monitor name=%q>: %s", k.Name, fmt.Sprintf(format, args...))
	}
	seenArg := set.New[string]()
	for _, c := range n.Children() {
		switch c.Name() {
		case "arg":
			if err := apidsl.CheckAttrs(c, "name", "required", "description"); err != nil {
				return nil, fail("%v", err)
			}
			a := MonitorArg{Name: c.Attr("name"), Description: c.Attr("description"), Required: apidsl.IsTruthy(c.Attr("required"))}
			if a.Name == "" {
				return nil, fail("<arg> needs name=")
			}
			if seenArg.Contains(a.Name) {
				return nil, fail("<arg name=%q> is declared twice", a.Name)
			}
			seenArg.Add(a.Name)
			k.Args = append(k.Args, a)
		case "poll", "stream":
			if k.Shape != "" {
				return nil, fail("has both <%s> and <%s>; a monitor has one check", k.Shape, c.Name())
			}
			if err := k.parseCheck(c); err != nil {
				return nil, fail("%v", err)
			}
		case "until":
			if k.until != "" {
				return nil, fail("<until> is set twice")
			}
			tmpl, err := compileElem(c)
			if err != nil {
				return nil, fail("<until>: %v", err)
			}
			k.until = tmpl
		case "describe":
			tmpl, err := compileElem(c)
			if err != nil {
				return nil, fail("<describe>: %v", err)
			}
			k.describe = tmpl
		default:
			return nil, fail("unknown element <%s>", c.Name())
		}
	}
	switch {
	case k.Shape == "":
		return nil, fail("needs a <poll> or a <stream>")
	case k.until == "" && (k.Shape == "stream" || k.Source == "request"):
		return nil, fail("a %s %s needs <until>: nothing else says when the condition holds", k.Source, k.Shape)
	}
	return k, nil
}

// compileElem compiles an element's content to a template, after it allows no attributes.
func compileElem(n *apidsl.Node) (string, error) {
	if err := apidsl.CheckAttrs(n); err != nil {
		return "", err
	}
	tmpl, err := apidsl.CompileContent(n)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(tmpl), nil
}

// parseCheck reads <poll> or <stream>. A poll holds one <request> or one or more <argv>. A stream holds only <argv>.
func (k *MonitorKind) parseCheck(n *apidsl.Node) error {
	k.Shape = n.Name()
	if k.Shape == "poll" {
		if err := apidsl.CheckAttrs(n, "every", "timeout"); err != nil {
			return err
		}
	} else if err := apidsl.CheckAttrs(n, "restart"); err != nil {
		return err
	}
	every, err := durationAttr(n, map[string]string{"poll": "every", "stream": "restart"}[k.Shape], DefaultMonitorEvery)
	if err != nil {
		return err
	}
	if every < minMonitorEvery {
		return fmt.Errorf("<%s>: the cadence %s is below the %s floor", k.Shape, every, minMonitorEvery)
	}
	k.Every = every
	if k.Shape == "poll" {
		if k.Timeout, err = durationAttr(n, "timeout", DefaultMonitorTimeout); err != nil {
			return err
		}
	}
	for _, it := range n.Content() {
		if it.IsText() {
			if strings.TrimSpace(it.Text) != "" {
				return fmt.Errorf("<%s> holds text %q: a check is a <request> or <argv> elements, never a shell string", k.Shape, strings.TrimSpace(it.Text))
			}
			continue
		}
		c := it.Elem
		switch c.Name() {
		case "argv":
			if k.request != nil {
				return fmt.Errorf("<%s> has both <request> and <argv>: a check is one or the other", k.Shape)
			}
			tmpl, err := compileElem(c)
			if err != nil {
				return fmt.Errorf("<argv>: %w", err)
			}
			k.argv = append(k.argv, tmpl)
			k.Source = "argv"
		case "request":
			if k.Shape == "stream" {
				return errors.New("<stream> holds only <argv>: a request has no output stream")
			}
			if k.request != nil || len(k.argv) > 0 {
				return errors.New("<poll> holds one <request>, or <argv> elements, not both and not two requests")
			}
			req, err := parseRequest(c)
			if err != nil {
				return err
			}
			k.request = req
			k.Source = "request"
		default:
			return fmt.Errorf("<%s>: unknown element <%s>", k.Shape, c.Name())
		}
	}
	if k.Source == "" {
		return fmt.Errorf("<%s> is empty: it needs a <request> or <argv> elements", k.Shape)
	}
	return nil
}

func parseRequest(n *apidsl.Node) (*monitorRequest, error) {
	if err := apidsl.CheckAttrs(n, "method", "transport"); err != nil {
		return nil, err
	}
	r := &monitorRequest{method: strings.ToUpper(n.Attr("method")), transport: n.Attr("transport")}
	if r.method == "" {
		r.method = "GET"
	}
	for _, c := range n.Children() {
		switch c.Name() {
		case "url":
			tmpl, err := compileElem(c)
			if err != nil {
				return nil, fmt.Errorf("<url>: %w", err)
			}
			r.url = tmpl
		case "header":
			if err := apidsl.CheckAttrs(c, "name"); err != nil {
				return nil, err
			}
			tmpl, err := apidsl.CompileContent(c)
			if err != nil {
				return nil, fmt.Errorf("<header>: %w", err)
			}
			if c.Attr("name") == "" {
				return nil, errors.New("<header> needs name=")
			}
			r.headers = append(r.headers, [2]string{c.Attr("name"), strings.TrimSpace(tmpl)})
		case "body":
			tmpl, err := compileElem(c)
			if err != nil {
				return nil, fmt.Errorf("<body>: %w", err)
			}
			r.body = tmpl
		default:
			return nil, fmt.Errorf("<request>: unknown element <%s>", c.Name())
		}
	}
	if r.url == "" {
		return nil, errors.New("<request> needs <url>")
	}
	return r, nil
}

func durationAttr(n *apidsl.Node, name string, def time.Duration) (time.Duration, error) {
	if !n.HasAttr(name) {
		return def, nil
	}
	d, err := time.ParseDuration(n.Attr(name))
	if err != nil {
		return 0, fmt.Errorf("<%s %s=%q>: %w", n.Name(), name, n.Attr(name), err)
	}
	return d, nil
}

// Kind returns one definition.
func (d *MonitorDefs) Kind(name string) (*MonitorKind, bool) {
	if d == nil {
		return nil, false
	}
	k, ok := d.kinds[name]
	return k, ok
}

// Kinds returns every definition, sorted by name.
func (d *MonitorDefs) Kinds() []*MonitorKind {
	if d == nil {
		return nil
	}
	out := make([]*MonitorKind, 0, len(d.kinds))
	for _, k := range d.kinds {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Validate checks a spec against its definition: the kind exists, every required arg is set, and no arg is undeclared.
func (d *MonitorDefs) Validate(spec MonitorSpec) error {
	k, ok := d.Kind(spec.Kind)
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownMonitorKind, spec.Kind)
	}
	declared := set.New[string]()
	for _, a := range k.Args {
		declared.Add(a.Name)
		if a.Required && spec.Args[a.Name] == "" {
			return fmt.Errorf("%w: %s needs %s", ErrBadMonitorArgs, k.Name, a.Name)
		}
	}
	for name := range spec.Args {
		if !declared.Contains(name) {
			return fmt.Errorf("%w: %s declares no arg %q", ErrBadMonitorArgs, k.Name, name)
		}
	}
	return nil
}
