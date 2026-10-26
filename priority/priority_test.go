package priority

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hookKeys struct {
	Name  string
	Infra MultiPriorityKey `priority:"infra_priority,default=50"`
	User  MultiPriorityKey `priority:"user"`
	Plain MultiPriorityKey
}

func TestFromFieldsReadsTaggedFieldsInOrder(t *testing.T) {
	var h hookKeys
	keys, err := FromFields(&h)
	require.NoError(t, err)
	list := keys.List()
	require.Len(t, list, 3)
	assert.Equal(t, []string{"infra_priority", "user", "plain"}, []string{list[0].Name, list[1].Name, list[2].Name})
	assert.Equal(t, 50, h.Infra.DefaultPriority)
	assert.Same(t, &h.User, keys.Lookup("user"))
}

func TestFromFieldsRefusesABadTag(t *testing.T) {
	type bad struct {
		K MultiPriorityKey `priority:"k,default=1.5"`
	}
	_, err := FromFields(&bad{})
	require.ErrorIs(t, err, ErrBadKey)
	type dup struct {
		A MultiPriorityKey `priority:"same"`
		B MultiPriorityKey `priority:"same"`
	}
	_, err = FromFields(&dup{})
	require.ErrorIs(t, err, ErrBadKey)
	_, err = FromFields(hookKeys{})
	require.ErrorIs(t, err, ErrBadKey, "a value is not a pointer")
}

func TestZeroValueIsEveryDefault(t *testing.T) {
	var h hookKeys
	keys, err := FromFields(&h)
	require.NoError(t, err)
	var p MultiPriority
	assert.Equal(t, 50, p.Get(&h.Infra))
	assert.Equal(t, 0, keys.Compare(p, keys.New()))
	assert.Equal(t, map[string]int{"infra_priority": 50, "user": 0, "plain": 0}, keys.Values(p))
}

func TestCompareLowerFirstAndTheFirstDifferenceDecides(t *testing.T) {
	var h hookKeys
	keys, err := FromFields(&h)
	require.NoError(t, err)
	a := keys.New()
	a.Set(&h.User, -1000)
	a.Set(&h.Infra, 25)
	b := keys.New()
	b.Set(&h.User, 0)
	assert.Equal(t, -1, keys.Compare(a, b), "infra 25 beats infra 50 whatever user says")
	c := keys.New()
	c.Set(&h.User, -1)
	assert.Equal(t, 1, keys.Compare(b, c), "a tie on infra goes to user")
	assert.Equal(t, 0, keys.Compare(c, c))
}

func TestSetDoesNotChangeACopy(t *testing.T) {
	var h hookKeys
	_, err := FromFields(&h)
	require.NoError(t, err)
	var p MultiPriority
	p.Set(&h.User, 3)
	q := p
	q.Set(&h.User, 9)
	assert.Equal(t, 3, p.Get(&h.User))
	assert.Equal(t, 9, q.Get(&h.User))
}

func TestFromValues(t *testing.T) {
	var h hookKeys
	keys, err := FromFields(&h)
	require.NoError(t, err)
	p, err := keys.FromValues(map[string]int{"user": 7})
	require.NoError(t, err)
	assert.Equal(t, 7, p.Get(&h.User))
	_, err = keys.FromValues(map[string]int{"nope": 1})
	require.ErrorIs(t, err, ErrUnknownKey)
}
