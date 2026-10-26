// Take picks the queued task of a consumer inbox that sorts first, and waits for its admission.

package jobserver

import (
	"context"
	"encoding/json"
	"time"

	"github.com/wow-look-at-my/go-jobserver/priority"
)

// candidate is a queued task of the inbox, ranked for Take.
type candidate struct {
	id      string
	prio    priority.MultiPriority
	created time.Time
}

// before reports whether a sorts ahead of b: lower priority, then arrival, then id.
func (c *inboxCore) before(a, b candidate) bool {
	if cmp := c.keys.Compare(a.prio, b.prio); cmp != 0 {
		return cmp < 0
	}
	if !a.created.Equal(b.created) {
		return a.created.Before(b.created)
	}
	return a.id < b.id
}

// queued lists the inbox's tasks whose trigger fired and that have not started. It reads the store, so call it without c.mu.
// It reads only the tasks of this inbox.
func (c *inboxCore) queued() []candidate {
	var out []candidate
	for _, r := range c.s.store.readyIn(c.name) {
		prio, err := c.keys.FromValues(r.priority)
		if err != nil {
			continue
		}
		out = append(out, candidate{id: r.id, prio: prio, created: r.created})
	}
	return out
}

// best is the queued task that sorts first, admitted or not. Caller holds c.mu.
func (c *inboxCore) best(queued []candidate) (candidate, bool) {
	var b candidate
	found := false
	consider := func(k candidate) {
		if !found || c.before(k, b) {
			b, found = k, true
		}
	}
	for _, k := range queued {
		if c.out[k.id] == nil {
			consider(k)
		}
	}
	for _, g := range c.admitted {
		consider(candidate{id: g.id, prio: g.prio, created: g.created})
	}
	return b, found
}

// yield helps the best queued task to its admission. Once that task waits in the scheduler, each admitted task
// that sorts after it gives back its lease and is held out of the scheduler. It reports whether the server must offer tasks again. Caller holds c.mu.
func (c *inboxCore) yield(best candidate) bool {
	if c.held[best.id] {
		delete(c.held, best.id)
		return true
	}
	if _, waiting := c.leasing[best.id]; !waiting {
		// The next tick offers it.
		return false
	}
	for id, g := range c.admitted {
		if c.before(best, candidate{id: g.id, prio: g.prio, created: g.created}) {
			g.lease.Release()
			delete(c.admitted, id)
			c.held[id] = true
		}
	}
	return false
}

// take checks out the queued task that sorts first. It waits for the
// admission of that task, not of another.
func (c *inboxCore) take(ctx context.Context, who Consumer, wait time.Duration) (string, json.RawMessage, bool, error) {
	var deadline <-chan time.Time
	if wait > 0 {
		t := time.NewTimer(wait)
		defer t.Stop()
		deadline = t.C
	}
	for {
		queued := c.queued()
		c.mu.Lock()
		if who == "" || c.consumer != who {
			c.mu.Unlock()
			return "", nil, false, ErrNotConsumer
		}
		wake := false
		if b, ok := c.best(queued); ok {
			if g := c.admitted[b.id]; g != nil {
				delete(c.admitted, g.id)
				if err := c.s.store.startTaken(g.id, string(who), c.consumerName); err != nil {
					c.mu.Unlock()
					g.lease.Release()
					continue
				}
				g.consumer = who
				c.out[g.id] = g
				if g.timeout > 0 {
					g.timer = time.AfterFunc(g.timeout, func() { c.expire(g.id) })
				}
				wake = len(c.held) > 0
				clear(c.held)
				c.mu.Unlock()
				if wake {
					c.s.Wake()
				}
				return g.id, g.spec, true, nil
			}
			wake = c.yield(b)
		}
		ch := c.avail
		c.mu.Unlock()
		if wake {
			c.s.Wake()
		}
		if wait == 0 {
			return "", nil, false, nil
		}
		select {
		case <-ch:
		case <-deadline:
			return "", nil, false, nil
		case <-ctx.Done():
			return "", nil, false, ctx.Err()
		}
	}
}
