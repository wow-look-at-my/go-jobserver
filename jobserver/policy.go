package jobserver

import (
	"bytes"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
)

// A Selector names processes by executable name or by process id.
type Selector struct {
	Names []string `json:"names,omitempty"`
	PIDs  []int    `json:"pids,omitempty"`
}

// Empty reports whether the selector names no process.
func (s Selector) Empty() bool { return len(s.Names) == 0 && len(s.PIDs) == 0 }

// Matches reports whether p is one of the selected processes. A name matches
// the process's own name and its base name, so both "ffmpeg" and a full path
// select the same process.
func (s Selector) Matches(p Proc) bool {
	for _, pid := range s.PIDs {
		if pid == p.PID {
			return true
		}
	}
	if len(s.Names) == 0 {
		return false
	}
	base := filepath.Base(p.Name)
	for _, name := range s.Names {
		if name == p.Name || name == base {
			return true
		}
	}
	return false
}

// String renders the selector in the form ParseSelector reads back.
func (s Selector) String() string {
	parts := make([]string, 0, len(s.PIDs)+len(s.Names))
	for _, pid := range s.PIDs {
		parts = append(parts, strconv.Itoa(pid))
	}
	parts = append(parts, s.Names...)
	return strings.Join(parts, ",")
}

// ParseSelector reads a comma-separated list of process ids and names. A token
// of digits is a pid; anything else is a name.
func ParseSelector(text string) Selector {
	var s Selector
	for _, tok := range strings.Split(text, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		if pid, err := strconv.Atoi(tok); err == nil {
			s.PIDs = append(s.PIDs, pid)
			continue
		}
		s.Names = append(s.Names, tok)
	}
	return s.normalized()
}

// normalized trims, de-duplicates and sorts a selector.
func (s Selector) normalized() Selector {
	out := Selector{}
	for _, name := range s.Names {
		name = strings.TrimSpace(name)
		if name != "" {
			out.Names = append(out.Names, name)
		}
	}
	out.Names = sortedUnique(out.Names)
	for _, pid := range s.PIDs {
		if pid > 0 {
			out.PIDs = append(out.PIDs, pid)
		}
	}
	sort.Ints(out.PIDs)
	out.PIDs = dedupInts(out.PIDs)
	return out
}

// ParseCPUs reads a comma-separated CPU list. The words "all" and "last" are
// shorthand for every CPU of the host and for its last one.
func ParseCPUs(text string) []int {
	switch strings.TrimSpace(text) {
	case "last":
		return nil
	case "all":
		cpus := runtime.NumCPU()
		out := make([]int, 0, cpus)
		for c := 0; c < cpus; c++ {
			out = append(out, c)
		}
		return out
	}
	var out []int
	for _, tok := range strings.Split(text, ",") {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}
		c, err := strconv.Atoi(tok)
		if err != nil {
			continue
		}
		out = append(out, c)
	}
	return validCPUs(out)
}

// Mechanism names one control the daemon can apply to a running job's processes.
type Mechanism string

const (
	// MechAffinity pins a process to a set of CPUs while its job is over budget.
	MechAffinity Mechanism = "affinity"
	// MechPriority raises a process's nice value while its job is over budget.
	MechPriority Mechanism = "priority"
	// MechFreeze suspends a process while its job is over budget.
	MechFreeze Mechanism = "freeze"
)

// MechanismOrder is the order the mechanisms are applied and reverted in: gentlest first.
var MechanismOrder = []Mechanism{MechAffinity, MechPriority, MechFreeze}

// Mechanisms lists the mechanisms this build knows.
func Mechanisms() []Mechanism { return append([]Mechanism(nil), MechanismOrder...) }

// ValidMechanism reports whether name is a mechanism this build knows.
func ValidMechanism(name string) bool {
	for _, m := range MechanismOrder {
		if string(m) == name {
			return true
		}
	}
	return false
}

// AffinitySettings pins a job's processes to a set of CPUs.
type AffinitySettings struct {
	Enabled bool     `json:"enabled,omitempty"`
	CPUs    []int    `json:"cpus,omitempty"`
	Exempt  Selector `json:"exempt,omitzero"`
	Only    Selector `json:"only,omitzero"`
}

// PrioritySettings changes a job's processes' nice value.
type PrioritySettings struct {
	Enabled bool     `json:"enabled,omitempty"`
	Nice    int      `json:"nice,omitempty"`
	Exempt  Selector `json:"exempt,omitzero"`
	Only    Selector `json:"only,omitzero"`
}

// FreezeSettings suspends a job's processes.
type FreezeSettings struct {
	Enabled bool     `json:"enabled,omitempty"`
	Exempt  Selector `json:"exempt,omitzero"`
	Only    Selector `json:"only,omitzero"`
}

// JobPolicy is a job's CPU declaration and the controls the daemon may apply
// to its processes while its job is over the CPU budget.
type JobPolicy struct {
	// Cost is how many CPUs the job is expected to use while it runs.
	Cost     float64          `json:"cost,omitempty"`
	Affinity AffinitySettings `json:"affinity"`
	Priority PrioritySettings `json:"priority"`
	Freeze   FreezeSettings   `json:"freeze"`
}

// Enabled lists the mechanisms this policy turns on, in application order.
func (p JobPolicy) Enabled() []Mechanism {
	var out []Mechanism
	for _, m := range MechanismOrder {
		if p.settings(m).Enabled {
			out = append(out, m)
		}
	}
	return out
}

// equal reports whether policies ask for the same thing.
func (p JobPolicy) equal(other JobPolicy) bool {
	var a, b encoder
	a.policy(p)
	b.policy(other)
	return bytes.Equal(a.b, b.b)
}

// clone returns a copy that shares no slice with the original.
func (p JobPolicy) clone() JobPolicy {
	out := p
	out.Affinity.CPUs = append([]int(nil), p.Affinity.CPUs...)
	out.Affinity.Exempt = p.Affinity.Exempt.normalized()
	out.Affinity.Only = p.Affinity.Only.normalized()
	out.Priority.Exempt = p.Priority.Exempt.normalized()
	out.Priority.Only = p.Priority.Only.normalized()
	out.Freeze.Exempt = p.Freeze.Exempt.normalized()
	out.Freeze.Only = p.Freeze.Only.normalized()
	return out
}

// Zero reports whether the policy asks for nothing.
func (p JobPolicy) Zero() bool {
	return p.Cost == 0 && p.Affinity.zero() && p.Priority.zero() && p.Freeze.zero()
}

// zero reports whether the settings block asks for nothing.
func (s AffinitySettings) zero() bool {
	return !s.Enabled && len(s.CPUs) == 0 && s.Exempt.Empty() && s.Only.Empty()
}

// zero reports whether the settings block asks for nothing.
func (s PrioritySettings) zero() bool {
	return !s.Enabled && s.Nice == 0 && s.Exempt.Empty() && s.Only.Empty()
}

// zero reports whether the settings block asks for nothing.
func (s FreezeSettings) zero() bool {
	return !s.Enabled && s.Exempt.Empty() && s.Only.Empty()
}

// Settings reports one mechanism's configuration: whether the job turns it
// on, the processes it spares, and the ones it is restricted to.
func (p JobPolicy) Settings(m Mechanism) (enabled bool, exempt, only Selector) {
	s := p.settings(m)
	return s.Enabled, s.Exempt, s.Only
}

// settings returns the settings block for one mechanism.
func (p JobPolicy) settings(m Mechanism) mechanismSettings {
	switch m {
	case MechAffinity:
		return mechanismSettings{Enabled: p.Affinity.Enabled, Exempt: p.Affinity.Exempt, Only: p.Affinity.Only}
	case MechPriority:
		return mechanismSettings{Enabled: p.Priority.Enabled, Exempt: p.Priority.Exempt, Only: p.Priority.Only}
	case MechFreeze:
		return mechanismSettings{Enabled: p.Freeze.Enabled, Exempt: p.Freeze.Exempt, Only: p.Freeze.Only}
	}
	return mechanismSettings{}
}

// mechanismSettings is the part of every mechanism's configuration that the
// selectors share.
type mechanismSettings struct {
	Enabled bool
	Exempt  Selector
	Only    Selector
}

// selects reports whether a process is subject to a mechanism: named by Only
// when Only is set, and never named by Exempt.
func (m mechanismSettings) selects(p Proc) bool {
	if m.Exempt.Matches(p) {
		return false
	}
	if m.Only.Empty() {
		return true
	}
	return m.Only.Matches(p)
}

// selected filters processes down to the ones a mechanism applies to.
func (m mechanismSettings) selected(procs []Proc) []Proc {
	out := make([]Proc, 0, len(procs))
	for _, p := range procs {
		if m.selects(p) {
			out = append(out, p)
		}
	}
	return out
}

// defaultNice is the nice value applied to a job that enables the priority mechanism without naming one.
const defaultNice = 10

// normalize fills in the defaults a policy's settings imply and drops
// contradictions, so a stored policy is exactly what the daemon acts on.
func (p JobPolicy) normalize() JobPolicy {
	out := p
	if out.Cost < 0 {
		out.Cost = 0
	}
	if out.Priority.Enabled && out.Priority.Nice == 0 {
		out.Priority.Nice = defaultNice
	}
	out.Priority.Nice = clampNice(out.Priority.Nice)
	if out.Affinity.Enabled {
		out.Affinity.CPUs = validCPUs(out.Affinity.CPUs)
	}
	out.Affinity.Exempt = out.Affinity.Exempt.normalized()
	out.Affinity.Only = out.Affinity.Only.normalized()
	out.Priority.Exempt = out.Priority.Exempt.normalized()
	out.Priority.Only = out.Priority.Only.normalized()
	out.Freeze.Exempt = out.Freeze.Exempt.normalized()
	out.Freeze.Only = out.Freeze.Only.normalized()
	return out
}

// clampNice keeps a nice value inside the range setpriority accepts.
func clampNice(nice int) int {
	if nice > 20 {
		return 20
	}
	if nice < -20 {
		return -20
	}
	return nice
}

// validCPUs drops negative CPU indices and de-duplicates the rest.
func validCPUs(cpus []int) []int {
	out := make([]int, 0, len(cpus))
	for _, c := range cpus {
		if c >= 0 {
			out = append(out, c)
		}
	}
	sort.Ints(out)
	return dedupInts(out)
}

// affinityCPUs is the CPU set a job's affinity mechanism pins to: the ones it
// named, or the last CPU of the host.
func affinityCPUs(settings AffinitySettings, cpus int) []int {
	if len(settings.CPUs) > 0 {
		return append([]int(nil), settings.CPUs...)
	}
	if cpus <= 0 {
		return nil
	}
	return []int{cpus - 1}
}

func sortedUnique(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := append([]string(nil), in...)
	sort.Strings(out)
	dedup := out[:0]
	for i, v := range out {
		if i == 0 || v != out[i-1] {
			dedup = append(dedup, v)
		}
	}
	return dedup
}

func dedupInts(in []int) []int {
	if len(in) == 0 {
		return nil
	}
	out := in[:1]
	for _, v := range in[1:] {
		if v != out[len(out)-1] {
			out = append(out, v)
		}
	}
	return out
}
