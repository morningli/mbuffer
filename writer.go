package mbuffer

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type BufferWriter struct {
	b          *Buffer
	activePage []byte // 当前物理页引用
	activeOff  int    // 当前页内偏移
	pageMax    int    // 当前页边界
	pageIdx    int    // 当前在 b.big 中的索引 (-1 表示在 SmallChunk)
	version    uint32
}

// NewWriter 初始化并执行第一次同步
func (b *Buffer) NewWriter() *BufferWriter {
	w := &BufferWriter{b: b}
	w.Sync()
	return w
}

// Sync 只更新 Writer 状态，不修改 Buffer 任何字段（无 Side Effect）
func (w *BufferWriter) Sync() {
	pLen := w.b.length + w.b.firstPageOffset

	// 路径 1: Small 阶段
	if w.b.hasSmall && pLen < SmallChunkSize {
		w.pageIdx = -1 // 标记在 Small
		if w.b.small != nil {
			w.activePage = w.b.small[:]
			w.activeOff = pLen
			w.pageMax = SmallChunkSize
		} else {
			w.activePage = nil
			w.activeOff = pLen
			w.pageMax = 0
		}
		w.version = w.b.version
		return
	}

	// 路径 2: Big 阶段
	prefix := 0
	if w.b.hasSmall {
		prefix = SmallChunkSize
	}
	bigPos := pLen - prefix
	w.pageIdx = bigPos >> bigShift

	if w.pageIdx < len(w.b.big) {
		w.activePage = w.b.big[w.pageIdx][:]
		w.activeOff = bigPos & bigMask
		w.pageMax = ChunkSize
	} else {
		w.activePage = nil
		w.activeOff = 0
		w.pageMax = 0
	}
	w.version = w.b.version
}

// WriteByte 极致优化的单字节写入
func (w *BufferWriter) WriteByte(c byte) error {
	if w.version != w.b.version {
		panic("writer is invalid")
	}
	if w.activeOff < w.pageMax {
		w.activePage[w.activeOff] = c
		w.activeOff++
		w.b.length++
		return nil
	}
	// 当前页满，进入跨页处理
	_, err := w.writeSlow([]byte{c})
	return err
}

// Write 批量写入
func (w *BufferWriter) Write(p []byte) (int, error) {
	if w.version != w.b.version {
		panic("writer is invalid")
	}
	n := len(p)
	// Fast Path
	if n <= w.pageMax-w.activeOff {
		copy(w.activePage[w.activeOff:], p)
		w.activeOff += n
		w.b.length += n
		return n, nil
	}
	// Slow Path
	return w.writeSlow(p)
}

// writeSlow 内部不再调用 Sync，而是由 Writer 自行处理物理页跳转
// writeSlow 处理跨页或首次写入的慢路径。
// 它依赖入口处的一次性空间预留，确保循环内的物理页跳转绝对安全。
func (w *BufferWriter) writeSlow(p []byte) (int, error) {
	total := len(p)
	w.b.ensureCapacity(total)
	w.Sync()

	srcOff := 0
	for srcOff < total {
		if w.activeOff >= w.pageMax {
			w.advancePage()
		}

		todo := w.pageMax - w.activeOff
		if rem := total - srcOff; rem < todo {
			todo = rem
		}

		copy(w.activePage[w.activeOff:], p[srcOff:srcOff+todo])

		w.activeOff += todo
		w.b.length += todo
		srcOff += todo
	}
	return total, nil
}

// advancePage 仅负责物理位置跳转。
// 所有的资源预留、模式切换（hasSmall = false）均已在入口处的 ensureCapacity 完成。
// go:inline
func (w *BufferWriter) advancePage() {
	w.pageIdx++
	w.activePage = w.b.big[w.pageIdx][:]
	w.activeOff = 0
	w.pageMax = ChunkSize
}

// CopyN 从 rd 中读取正好 n 个字节灌入当前活跃页。
func (w *BufferWriter) CopyN(rd io.Reader, n int) error {
	if w.version != w.b.version {
		panic("writer is invalid")
	}
	// 1. 唯一的一次物理决策和资源预留点
	w.b.ensureCapacity(n)
	w.Sync()

	read := 0
	for read < n {
		// 2. 磁头驱动：如果没页或写满了，直接挪到下一页
		// 此时 w.b.big[w.pageIdx+1] 已经由 ensureCapacity 保证存在
		if w.activeOff >= w.pageMax {
			w.advancePage()
		}

		// 3. 计算当前物理页可承载的长度
		limit := w.pageMax - w.activeOff
		remaining := n - read
		if remaining < limit {
			limit = remaining
		}

		// 4. 直接读入当前活跃物理页切片，没有任何位运算
		nr, err := rd.Read(w.activePage[w.activeOff : w.activeOff+limit])
		if nr > 0 {
			w.activeOff += nr
			w.b.length += nr
			read += nr
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// CopyBufferedTo 尽可能多地从 bufio 缓冲区读取数据并拷贝到 w 中。
func (w *BufferWriter) CopyBufferedTo(rd *bufio.Reader) (int64, error) {
	if w.version != w.b.version {
		panic("writer is invalid")
	}
	n := rd.Buffered()
	if n > 0 {
		// 直接复用 CopyN 逻辑
		err := w.CopyN(rd, n)
		return int64(n), err
	}

	// 缓冲区为空，进入物理读取慢路径
	return w.copyBufferedSlow(rd)
}

//go:noinline
func (w *BufferWriter) copyBufferedSlow(rd *bufio.Reader) (int64, error) {
	var total int64
	for {
		// 1. 至少预留 1 字节空间触发物理页准备
		w.b.ensureCapacity(1)
		w.Sync()

		// 2. 磁头驱动
		if w.activeOff >= w.pageMax {
			w.advancePage()
		}

		limit := w.pageMax - w.activeOff
		buffered := rd.Buffered()

		if buffered > 0 {
			// 缓冲区有预读数据，执行收割
			if limit > buffered {
				limit = buffered
			}
			nr, _ := rd.Read(w.activePage[w.activeOff : w.activeOff+limit])
			if nr > 0 {
				w.activeOff += nr
				w.b.length += nr
				total += int64(nr)
			}
		} else {
			// 缓冲区为空，物理读取触发 bufio 填充
			nr, err := rd.Read(w.activePage[w.activeOff : w.activeOff+limit])
			if nr > 0 {
				w.activeOff += nr
				w.b.length += nr
				total += int64(nr)
			}
			if err != nil {
				return total, err
			}
		}

		if rd.Buffered() == 0 {
			break
		}
	}
	return total, nil
}

func (w *BufferWriter) String() string {
	buff := strings.Builder{}
	buff.WriteString("activePage:")
	buff.WriteString(fmt.Sprintf("[]byte(%d)", len(w.activePage)))
	buff.WriteString(" activeOff:")
	buff.WriteString(fmt.Sprintf("%d", w.activeOff))
	buff.WriteString(" pageMax:")
	buff.WriteString(fmt.Sprintf("%d", w.pageMax))
	buff.WriteString(" pageIdx:")
	buff.WriteString(fmt.Sprintf("%d", w.pageIdx))
	buff.WriteString(" version:")
	buff.WriteString(fmt.Sprintf("%d", w.version))
	return buff.String()
}
