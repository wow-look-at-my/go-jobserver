package jobserver

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFinishClosesTheJobsLogFile(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	_, err = st.Create(&Spec{ID: "j-1", Command: []string{"true"}})
	require.NoError(t, err)
	w, err := st.LogWriter("j-1")
	require.NoError(t, err)
	_, err = w.Write([]byte("one\n"))
	require.NoError(t, err)

	require.NoError(t, st.Finish("j-1", StateCompleted, 0, "", nil, w.N()))

	assert.Empty(t, st.logs, "Finish kept the writer in the cache")
	_, err = w.Write([]byte("late\n"))
	assert.ErrorIs(t, err, os.ErrClosed, "Finish left the log file open")
}

func TestALogOpenedAfterFinishAppendsToTheSameFile(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	_, err = st.Create(&Spec{ID: "j-1", Command: []string{"true"}})
	require.NoError(t, err)
	first, err := st.LogWriter("j-1")
	require.NoError(t, err)
	_, err = first.Write([]byte("one\n"))
	require.NoError(t, err)
	require.NoError(t, st.Finish("j-1", StateCompleted, 0, "", nil, first.N()))

	second, err := st.LogWriter("j-1")
	require.NoError(t, err)
	assert.NotSame(t, first, second, "a finished job's writer came back from the cache")
	_, err = second.Write([]byte("two\n"))
	require.NoError(t, err)
	require.NoError(t, second.Sync())

	out, err := st.ReadLog("j-1", 0, 1024)
	require.NoError(t, err)
	assert.Equal(t, "one\ntwo\n", string(out))
}

func TestAReopenedLogWriterCountsTheBytesAlreadyInTheFile(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	_, err = st.Create(&Spec{ID: "j-1", Command: []string{"true"}})
	require.NoError(t, err)
	first, err := st.LogWriter("j-1")
	require.NoError(t, err)
	_, err = first.Write([]byte("one\n"))
	require.NoError(t, err)
	require.NoError(t, st.Finish("j-1", StateCompleted, 0, "", nil, first.N()))

	second, err := st.LogWriter("j-1")
	require.NoError(t, err)
	_, err = second.Write([]byte("two\n"))
	require.NoError(t, err)

	assert.EqualValues(t, 8, second.N())
	assert.Equal(t, st.LogBytes("j-1"), second.N(), "the recorded size must match the file")
}

func TestSyncLogNeverOpensALog(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	_, err = st.Create(&Spec{ID: "j-1", Command: []string{"true"}})
	require.NoError(t, err)

	require.NoError(t, st.SyncLog("j-1"))
	assert.Empty(t, st.logs, "SyncLog cached a writer")
	assert.NoFileExists(t, st.LogPath("j-1"), "SyncLog created the log file")

	w, err := st.LogWriter("j-1")
	require.NoError(t, err)
	_, err = w.Write([]byte("x"))
	require.NoError(t, err)
	require.NoError(t, st.SyncLog("j-1"))
	assert.Len(t, st.logs, 1)

	require.NoError(t, st.Finish("j-1", StateCompleted, 0, "", nil, w.N()))
	require.NoError(t, st.SyncLog("j-1"))
	assert.Empty(t, st.logs, "SyncLog reopened the log of a finished job")
}

func TestSyncLogToleratesAFileThatClosedUnderIt(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	w, err := st.LogWriter("j-1")
	require.NoError(t, err)
	require.NoError(t, w.Close())

	assert.NoError(t, st.SyncLog("j-1"))
}

// An interrupt can cancel a job after the loop read it as ready. Its start must then refuse, or the cancelled job runs.
func TestStartRefusesAJobThatIsNoLongerActive(t *testing.T) {
	st, err := OpenStore(t.TempDir())
	require.NoError(t, err)
	defer st.Close()
	_, err = st.Create(&Spec{ID: "j-1", Command: []string{"true"}})
	require.NoError(t, err)
	require.NoError(t, st.Finish("j-1", StateCancelled, -1, "interrupted", nil, 0))
	require.ErrorIs(t, st.Start("j-1"), ErrBadState)
	j, ok := st.Job("j-1")
	require.True(t, ok)
	assert.Equal(t, StateCancelled, j.State, "the refused start leaves the job cancelled")
}
