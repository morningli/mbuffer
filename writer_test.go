package mbuffer

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBufferWriter_Sync(t *testing.T) {
	b := initBuffer(t, false, 1, 24319)
	wr := b.NewWriter()
	t.Logf("small:%p,big:%d", b.small, len(b.big))
	t.Logf("wr:%s", wr)
	require.Equal(t, 5, wr.pageIdx)
	require.Equal(t, 4096, wr.pageMax)
	require.Equal(t, 3840, wr.activeOff)

	target := NewBuffer()
	b.ShiftTo(4045, target)
	wr.Sync()

	//b2 := initBuffer(t, true, 206, 20274)
	//wr2 := b2.NewWriter()

	t.Logf("small:%p,big:%d", b.small, len(b.big))
	t.Logf("wr:%s", wr)
	require.Equal(t, 4, wr.pageIdx)
	require.Equal(t, 4096, wr.pageMax)
	require.Equal(t, 3840, wr.activeOff)
}
