package main

import (
	"errors"
	"flag"
	"fmt"
	"strings"

	"github.com/wow-look-at-my/go-jobserver/jobserver"
)

// setFlag is a string flag that remembers being given, so an empty value
// still counts as a request.
type setFlag struct {
	value string
	set   bool
}

func (f *setFlag) String() string { return f.value }

func (f *setFlag) Set(v string) error {
	f.value, f.set = v, true
	return nil
}

// policyFlags are the CPU policy flags queue and policy share.
type policyFlags struct {
	cost     float64
	nice     int
	priority bool
	freeze   bool
	affinity setFlag
	exempt   setFlag
	only     setFlag
	mech     map[jobserver.Mechanism]*[2]setFlag
}

// bind adds the policy flags.
func (p *policyFlags) bind(fs *flag.FlagSet) {
	p.mech = map[jobserver.Mechanism]*[2]setFlag{}
	fs.Float64Var(&p.cost, "cpu", 0, "CPUs the job is expected to use")
	fs.BoolVar(&p.priority, "priority", false, "raise the nice value while over budget")
	fs.IntVar(&p.nice, "nice", 0, "nice value for the priority mechanism")
	fs.BoolVar(&p.freeze, "freeze", false, "suspend processes while over budget")
	fs.Var(&p.affinity, "affinity", "CPUs to pin to (\"0,1\", \"all\" or \"last\")")
	fs.Var(&p.exempt, "exempt", "processes every enabled mechanism leaves alone")
	fs.Var(&p.only, "only", "processes every enabled mechanism is restricted to")
	for _, m := range jobserver.Mechanisms() {
		pair := new([2]setFlag)
		fs.Var(&pair[0], string(m)+"-exempt", "processes the "+string(m)+" mechanism leaves alone")
		fs.Var(&pair[1], string(m)+"-only", "processes the "+string(m)+" mechanism is restricted to")
		p.mech[m] = pair
	}
}

// policy turns the flags into a job policy.
func (p *policyFlags) policy() jobserver.JobPolicy {
	return jobserver.JobPolicy{
		Cost: p.cost,
		Affinity: jobserver.AffinitySettings{
			Enabled: p.affinity.set,
			CPUs:    jobserver.ParseCPUs(p.affinity.value),
			Exempt:  p.mechExempt(jobserver.MechAffinity),
			Only:    p.mechOnly(jobserver.MechAffinity),
		},
		Priority: jobserver.PrioritySettings{
			Enabled: p.priority || p.nice != 0,
			Nice:    p.nice,
			Exempt:  p.mechExempt(jobserver.MechPriority),
			Only:    p.mechOnly(jobserver.MechPriority),
		},
		Freeze: jobserver.FreezeSettings{
			Enabled: p.freeze,
			Exempt:  p.mechExempt(jobserver.MechFreeze),
			Only:    p.mechOnly(jobserver.MechFreeze),
		},
	}
}

// mechExempt is one mechanism's exemption list: its own flag when given,
// otherwise the job-wide one.
func (p *policyFlags) mechExempt(m jobserver.Mechanism) jobserver.Selector {
	if pair := p.mech[m]; pair[0].set {
		return jobserver.ParseSelector(pair[0].value)
	}
	return jobserver.ParseSelector(p.exempt.value)
}

// mechOnly is one mechanism's restriction list, by the same rule.
func (p *policyFlags) mechOnly(m jobserver.Mechanism) jobserver.Selector {
	if pair := p.mech[m]; pair[1].set {
		return jobserver.ParseSelector(pair[1].value)
	}
	return jobserver.ParseSelector(p.only.value)
}

// cmdPolicy replaces a job's CPU policy.
func cmdPolicy(g *globals, args []string) error {
	fs := flag.NewFlagSet("policy", flag.ContinueOnError)
	fs.Usage = func() { fmt.Print(usage) }
	g.bind(fs)
	var pol policyFlags
	pol.bind(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("policy needs one job id")
	}
	id := fs.Arg(0)
	policy := pol.policy()
	c, ctx, cancel, err := client(g)
	if err != nil {
		return err
	}
	defer cancel()
	defer c.Close()
	resp, err := c.Do(ctx, jobserver.Request{Op: jobserver.OpPolicy, ID: id, Policy: &policy})
	if err != nil {
		return err
	}
	if !resp.OK {
		return errors.New(resp.Error)
	}
	printPolicy(resp.Job.Policy)
	return nil
}

// printPolicy reports a job's policy in the form the flags take.
func printPolicy(p jobserver.JobPolicy) {
	fmt.Printf("cpu        %g\n", p.Cost)
	for _, m := range jobserver.Mechanisms() {
		enabled, exempt, only := p.Settings(m)
		line := "off"
		if enabled {
			switch m {
			case jobserver.MechPriority:
				line = fmt.Sprintf("on nice %d", p.Priority.Nice)
			case jobserver.MechAffinity:
				line = "on cpus " + joinCPUList(p.Affinity.CPUs)
			default:
				line = "on"
			}
		}
		if !exempt.Empty() {
			line += " exempt " + exempt.String()
		}
		if !only.Empty() {
			line += " only " + only.String()
		}
		fmt.Printf("%-10s %s\n", m, line)
	}
}

// joinCPUList renders a CPU list for the policy report.
func joinCPUList(cpus []int) string {
	if len(cpus) == 0 {
		return "last"
	}
	parts := make([]string, 0, len(cpus))
	for _, c := range cpus {
		parts = append(parts, fmt.Sprint(c))
	}
	return strings.Join(parts, ",")
}
