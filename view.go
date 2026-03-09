package mbuffer

// BufferView 是对 IndexedBuffer 部分片段的只读视图。
// 它通过引用原 Buffer 的页表并记录逻辑偏移来实现零拷贝操作。
type BufferView struct {
	length          int
	firstPageOffset int
	small           *SmallChunk
	big             []*Chunk
	version         uint32
	hasSmall        bool
}

// Len 返回视图的逻辑数据长度
func (v BufferView) Len() int {
	return v.length
}

// IsEmpty 返回视图是否为空
func (v BufferView) IsEmpty() bool {
	return v.length == 0
}

//go:inline At 支持随机访问，返回逻辑索引 index 处的字节
func (v BufferView) At(index int) byte {
	// 1. 极简边界检查：利用 uint 一次性判定 index < 0 || index >= v.length
	if uint(index) >= uint(v.length) {
		panic("Buffer: slice index out of range")
	}
	// 2. 调用物理索引函数（确保 atByte 也是内联的）
	return atByte(v.hasSmall, v.small, v.big, v.firstPageOffset, index)
}

//go:inline Data 将视图转换为不连续的字节切片列表。常用于 net.Buffers 或 writev 系统调用
func (v BufferView) Data() [][]byte {
	return dataSlices(v.hasSmall, v.small, v.big, v.firstPageOffset, v.length)
}

//go:inline Bytes 将视图内容转换为连续的切片。优化：针对单页场景返回底层引用（0 拷贝），仅在跨页时执行分配与合并。
func (v BufferView) Bytes() []byte {
	return dataSlice(v.hasSmall, v.small, v.big, v.firstPageOffset, v.length)
}

//go:inline Slice 在当前视图基础上再次切片 v[n:m]
func (v BufferView) Slice(n, m int) BufferView {
	// 1. 使用 uint 技巧一次性检查 n < 0, m < n, m > length
	// 这与 Go 编译器处理切片的底层逻辑一致，节点数最少
	if uint(n) > uint(m) || uint(m) > uint(v.length) {
		panic("Buffer: slice index out of range")
	}

	length := m - n
	physicalStart := n + v.firstPageOffset
	return sliceFromPhysical(v.hasSmall, v.small, v.big, physicalStart, length)
}

//go:inline Tail 返回从 n 到末尾的视图，等价于 Go 切片 v[n:].
func (v BufferView) Tail(n int) BufferView {
	return v.Slice(n, v.length)
}

func (v BufferView) IndexByte(c byte, start int) int {
	return indexByteGeneric(v.hasSmall, v.small, v.big, v.firstPageOffset, v.length, c, start)
}

// ParseInt BufferView 的成员函数 (内联)
func (v BufferView) ParseInt() (int, error) {
	return parseIntGeneric(v.hasSmall, v.small, v.big, v.firstPageOffset, v.length)
}
