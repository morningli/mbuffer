package mbuffer

import (
	"errors"
	"io"
	"sync"
)

var ErrInvalidInteger = errors.New("invalid integer")

const (
	// SmallChunkSize 表示小页（第一页）的字节大小。
	SmallChunkSize = 256
	// bigShift 表示大页大小的 2 次幂指数：$2^{12}=4096$。
	bigShift = 12
	// ChunkSize 表示大页（Chunk）的字节大小。
	ChunkSize = 1 << bigShift
	// bigMask 用于等价替代 `% ChunkSize`。
	bigMask          = 1<<bigShift - 1
	initBigPageCount = 8
)

// 注意：Buffer 采用“第一页小页、后续大页”的策略：
// - 第 0 页固定为 256B 小页
// - 当超过 256B 后，后续追加 4KB 大页

// Chunk 是 Buffer 使用的固定大小大页（从对象池复用）。
type Chunk [ChunkSize]byte

var chunkPool = sync.Pool{
	New: func() interface{} { return new(Chunk) },
}

func getBigChunk() *Chunk { return chunkPool.Get().(*Chunk) }
func putBigChunk(c *Chunk) {
	if c != nil {
		chunkPool.Put(c)
	}
}

// SmallChunk 是 Buffer 使用的固定大小小页（从对象池复用）。
type SmallChunk [SmallChunkSize]byte

var smallChunkPool = sync.Pool{
	New: func() interface{} { return new(SmallChunk) },
}

func getSmallChunk() *SmallChunk { return smallChunkPool.Get().(*SmallChunk) }
func putSmallChunk(c *SmallChunk) {
	if c != nil {
		smallChunkPool.Put(c)
	}
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		return &Buffer{
			hasSmall: true,
			capacity: 0, //
			big:      make([]*Chunk, 0, initBigPageCount),
		}
	},
}

// Buffer 是基于固定大小页的可增长字节缓冲区，支持零拷贝 Slice。
type Buffer struct {
	// length 逻辑上的总有效数据长度
	length int
	// firstPageOffset 第一页的起始有效数据偏移（0 ~ pageSize-1）
	firstPageOffset int
	// capacity 缓存当前 Buffer 总物理容量（包含 small 和所有 big）
	capacity int
	// small 仅用于第一页（256B）
	small *SmallChunk
	// big 保存后续所有 4KB 页；当 Buffer 起始页为大页时，big[0] 即第一页。
	big     []*Chunk
	version uint32
	// hasSmall 表示该 Buffer 的起始页是否为 small（256B）。NewBuffer 创建的 Buffer 恒为 true；Split 得到的 remainder 可能为 false（零拷贝所需）。
	hasSmall bool
}

// NewBuffer 创建一个空 Buffer。
func NewBuffer() *Buffer {
	return bufferPool.Get().(*Buffer)
}

//go:inline Slice 模拟 Go 原生切片操作 b[n:m],n: 起始位移 (inclusive),m: 结束位移 (exclusive)
func (b *Buffer) Slice(n, m int) BufferView {
	// 1. 使用 uint 技巧一次性检查 n < 0, m < n, m > length
	// 这与 Go 编译器处理切片的底层逻辑一致，节点数最少
	if uint(n) > uint(m) || uint(m) > uint(b.length) {
		panic("Buffer: slice index out of range")
	}

	length := m - n
	physicalStart := n + b.firstPageOffset
	// 2. 调用已优化的 sliceFromPhysical
	return sliceFromPhysical(b.hasSmall, b.small, b.big, physicalStart, length)
}

//go:inline Tail 返回从 n 到末尾的视图，等价于 Go 切片 b[n:].
func (b *Buffer) Tail(n int) BufferView {
	return b.Slice(n, b.length)
}

// Split 在 n 位置切断数据。
// 原有的 b 将保留 [0, n) 字节（完整指令，通过拷贝分割点实现物理隔离）。
// 返回的新 Buffer 将承接 [n, length) 字节（剩余流，通过位移实现零平移）。
func (b *Buffer) Split(n int, newBuf *Buffer) {
	if n <= 0 {
		// 情况：n=0，原 b 变为空，所有数据移交给新返回的 Buffer
		b.Swap(newBuf)
		//b.version++
		return
	}

	if n >= b.length {
		// 情况：n 超过长度，b 保留所有，返回一个空的 Buffer
		return
	}

	originalSmall := b.small
	originalBig := b.big
	originalLen := b.length
	originalFPO := b.firstPageOffset
	originalHasSmall := b.hasSmall

	splitPos := n + originalFPO
	newLen := originalLen - n

	// prefix is the size of the first page in bytes (small page when present).
	prefix := 0
	if originalHasSmall {
		prefix = SmallChunkSize
	}

	if originalHasSmall {
		// Split in small page
		if splitPos < SmallChunkSize {
			if originalSmall == nil {
				// Shouldn't happen: data exists but small nil. Be defensive.
				originalSmall = getSmallChunk()
				b.small = originalSmall
			}
			// b keeps the original small page (no copy); newBuf gets a fresh small page for suffix.
			b.small = originalSmall
			b.big = nil
			b.hasSmall = true
			b.length = n
			// b.firstPageOffset unchanged
			b.capacity = SmallChunkSize // b 仅剩 small

			// newBuf: allocate a new small page and copy the suffix bytes into the END part.
			suffixLen := min(SmallChunkSize-splitPos, newLen)
			suffix := originalSmall[splitPos : splitPos+suffixLen]
			newHasSmall, newSmall, newBigFirst, newFirstOff := makeSuffixFirstPage(suffix)
			newBuf.hasSmall = newHasSmall
			newBuf.small = newSmall
			newBuf.firstPageOffset = newFirstOff

			// 继承后续大页
			if len(newBigFirst) > 0 {
				// Should never happen since suffixLen <= SmallChunkSize, but keep consistent.
				newBuf.big = append(newBigFirst, originalBig...)
			} else {
				newBuf.big = originalBig
			}
			newBuf.length = newLen
			// 更新 newBuf.capacity: 计算方式同 ensureCapacity
			newBuf.capacity = calculateCapacity(newBuf.hasSmall, len(newBuf.big))
			b.version++
			return
		}

		// Boundary right after small page
		if splitPos == SmallChunkSize {
			b.small = originalSmall
			b.big = nil
			b.hasSmall = true
			b.length = n
			b.capacity = SmallChunkSize

			newBuf.small = nil
			newBuf.big = originalBig
			newBuf.hasSmall = false
			newBuf.firstPageOffset = 0
			newBuf.length = newLen
			newBuf.capacity = calculateCapacity(false, len(newBuf.big))
			b.version++
			return
		}
	}

	// Split in big pages (shared for hasSmall=true and hasSmall=false).
	bigSplitPos := splitPos - prefix
	splitBigIdx := bigSplitPos >> bigShift
	innerOff := bigSplitPos & bigMask

	// b keeps pages up to the boundary page; remainder never reuses the boundary page.
	b.small = originalSmall
	b.hasSmall = originalHasSmall
	if innerOff > 0 {
		b.big = append([]*Chunk(nil), originalBig[:splitBigIdx+1]...)
		b.capacity = prefix + (splitBigIdx+1)*ChunkSize
	} else {
		b.big = append([]*Chunk(nil), originalBig[:splitBigIdx]...)
		b.capacity = prefix + (splitBigIdx)*ChunkSize
	}
	b.length = n
	// b.firstPageOffset unchanged

	if innerOff == 0 {
		// Boundary on big page edge: remainder can take pages without copying.
		newBuf.small = nil
		newBuf.hasSmall = false
		newBuf.big = originalBig[splitBigIdx:]
		newBuf.firstPageOffset = 0
		newBuf.length = newLen
		newBuf.capacity = len(newBuf.big) * ChunkSize
		b.version++
		return
	}

	// Split within a big page: copy suffix to a fresh first page for newBuf.
	suffixInThisPage := min(ChunkSize-innerOff, newLen)
	suffix := originalBig[splitBigIdx][innerOff : innerOff+suffixInThisPage]
	newHasSmall, newSmall, newBigFirst, newFirstOff := makeSuffixFirstPage(suffix)
	newBuf.hasSmall = newHasSmall
	newBuf.small = newSmall
	newBuf.firstPageOffset = newFirstOff
	if splitBigIdx+1 < len(originalBig) {
		if len(newBigFirst) > 0 {
			newBuf.big = append(newBigFirst, originalBig[splitBigIdx+1:]...)
		} else {
			newBuf.big = originalBig[splitBigIdx+1:]
		}
	} else {
		newBuf.big = newBigFirst
	}
	newBuf.length = newLen
	newBuf.capacity = calculateCapacity(newBuf.hasSmall, len(newBuf.big))
	b.version++
	return
}

// ShiftTo 从 b 的头部切出 n 字节转移到 target，b 仅保留剩余部分。
// 这是解析 Redis 命令时提取 Raw 原始报文的高效路径。
func (b *Buffer) ShiftTo(n int, newBuf *Buffer) {
	if n <= 0 {
		// 情况：n=0，b 保留所有，返回一个空的 Buffer
		return
	}

	if n >= b.length {
		// 情况：n 超过长度，原 b 变为空，所有数据移交给新返回的 Buffer
		b.Swap(newBuf)
		//b.version++
		return
	}

	originalSmall := b.small
	originalBig := b.big
	originalLen := b.length
	originalFPO := b.firstPageOffset
	originalHasSmall := b.hasSmall

	splitPos := n + originalFPO
	newLen := originalLen - n

	// prefix is the size of the first page in bytes (small page when present).
	prefix := 0
	if originalHasSmall {
		prefix = SmallChunkSize
	}

	if originalHasSmall {
		// Split in small page
		if splitPos < SmallChunkSize {
			// b keeps the original small page (no copy); newBuf gets a fresh small page for suffix.
			newBuf.small = originalSmall
			newBuf.big = nil
			newBuf.hasSmall = true
			newBuf.length = n
			// b.firstPageOffset unchanged
			newBuf.firstPageOffset = originalFPO
			newBuf.capacity = SmallChunkSize // b 仅剩 small

			// newBuf: allocate a new small page and copy the suffix bytes into the END part.
			suffixLen := min(SmallChunkSize-splitPos, newLen)
			suffix := originalSmall[splitPos : splitPos+suffixLen]
			newHasSmall, newSmall, newBigFirst, newFirstOff := makeSuffixFirstPage(suffix)
			b.hasSmall = newHasSmall
			b.small = newSmall
			b.firstPageOffset = newFirstOff

			// 继承后续大页
			if len(newBigFirst) > 0 {
				// Should never happen since suffixLen <= SmallChunkSize, but keep consistent.
				b.big = append(newBigFirst, originalBig...)
			} else {
				b.big = originalBig
			}
			b.length = newLen
			// 更新 newBuf.capacity: 计算方式同 ensureCapacity
			b.capacity = calculateCapacity(newBuf.hasSmall, len(newBuf.big))
			b.version++
			return
		}

		// Boundary right after small page
		if splitPos == SmallChunkSize {
			newBuf.small = originalSmall
			newBuf.big = nil
			newBuf.hasSmall = true
			newBuf.length = n
			newBuf.capacity = SmallChunkSize
			newBuf.firstPageOffset = originalFPO

			b.small = nil
			b.big = originalBig
			b.hasSmall = false
			b.firstPageOffset = 0
			b.length = newLen
			b.capacity = calculateCapacity(false, len(newBuf.big))
			b.version++
			return
		}
	}

	// Split in big pages (shared for hasSmall=true and hasSmall=false).
	bigSplitPos := splitPos - prefix
	splitBigIdx := bigSplitPos >> bigShift
	innerOff := bigSplitPos & bigMask

	// b keeps pages up to the boundary page; remainder never reuses the boundary page.
	newBuf.small = originalSmall
	newBuf.hasSmall = originalHasSmall
	if innerOff > 0 {
		newBuf.big = append([]*Chunk(nil), originalBig[:splitBigIdx+1]...)
		newBuf.capacity = prefix + (splitBigIdx+1)*ChunkSize
	} else {
		newBuf.big = append([]*Chunk(nil), originalBig[:splitBigIdx]...)
		newBuf.capacity = prefix + (splitBigIdx)*ChunkSize
	}
	newBuf.length = n
	newBuf.firstPageOffset = originalFPO

	if innerOff == 0 {
		// Boundary on big page edge: remainder can take pages without copying.
		b.small = nil
		b.hasSmall = false
		b.big = originalBig[splitBigIdx:]
		b.firstPageOffset = 0
		b.length = newLen
		b.capacity = len(newBuf.big) * ChunkSize
		b.version++
		return
	}

	// Split within a big page: copy suffix to a fresh first page for newBuf.
	suffixInThisPage := min(ChunkSize-innerOff, newLen)
	suffix := originalBig[splitBigIdx][innerOff : innerOff+suffixInThisPage]
	newHasSmall, newSmall, newBigFirst, newFirstOff := makeSuffixFirstPage(suffix)
	b.hasSmall = newHasSmall
	b.small = newSmall
	b.firstPageOffset = newFirstOff
	if splitBigIdx+1 < len(originalBig) {
		if len(newBigFirst) > 0 {
			b.big = append(newBigFirst, originalBig[splitBigIdx+1:]...)
		} else {
			b.big = originalBig[splitBigIdx+1:]
		}
	} else {
		b.big = newBigFirst
	}
	b.length = newLen
	b.capacity = calculateCapacity(newBuf.hasSmall, len(newBuf.big))
	b.version++
	return
}

// Discard 从 Buffer 头部直接丢弃 n 字节数据，并立即释放不再被引用的物理大页。
func (b *Buffer) Discard(n int) {
	if n <= 0 {
		return
	}
	if n >= b.length {
		b.Reset()
		//b.version++
		return
	}

	// 1. 记录原始状态
	origFPO := b.firstPageOffset
	origHasSmall := b.hasSmall
	origBig := b.big

	// 计算物理上的总偏移點
	splitPos := n + origFPO
	b.length -= n

	if origHasSmall {
		// --- 情況 A: 丢弃后新起点仍在 Small 页內 ---
		if splitPos < SmallChunkSize {
			b.firstPageOffset = splitPos
			// 確保 hasSmall 保持為 true
			b.hasSmall = true
			b.version++
			return
		}

		// --- 情況 B: 跨过 Small 页進入 Big 区域 ---
		// 必須減去 SmallChunkSize。
		bigSplitPos := splitPos - SmallChunkSize
		splitBigIdx := bigSplitPos >> bigShift
		innerOff := bigSplitPos & bigMask

		// 物理回收被完全跳过的大页
		for i := 0; i < splitBigIdx; i++ {
			putBigChunk(origBig[i])
		}

		b.hasSmall = false
		b.firstPageOffset = innerOff
		b.big = origBig[splitBigIdx:]
		b.capacity = len(b.big) * ChunkSize
	} else {
		// --- 情況 C: 本來就在 Big 区域 ---
		splitBigIdx := splitPos >> bigShift
		innerOff := splitPos & bigMask

		for i := 0; i < splitBigIdx; i++ {
			putBigChunk(origBig[i])
		}

		b.firstPageOffset = innerOff
		b.big = origBig[splitBigIdx:]
		b.capacity = len(b.big) * ChunkSize
	}
	b.version++
}

// Free 释放内存
func (b *Buffer) Free() {
	// 注意：在 Split 场景下，多个 Buffer 可能引用同一个 Chunk
	// 这里简单的 Put 回池子仅适用于你确定该 Buffer 独占这些 Chunk 的情况
	// 如果需要严谨，需要引入引用计数。但在 gnet 解析完即销毁的场景，
	// 通常在处理完最后一个 Split 块后统一释放即可。
	if b.hasSmall && b.small != nil {
		putSmallChunk(b.small)
	}
	b.small = nil
	for i := range b.big {
		putBigChunk(b.big[i])
	}
	if cap(b.big) == initBigPageCount {
		b.big = b.big[:0]
	} else {
		b.big = make([]*Chunk, 0, initBigPageCount)
	}
	b.length = 0
	b.firstPageOffset = 0
	b.hasSmall = true
	b.capacity = 0
	b.version = 0
	bufferPool.Put(b)
}

func (b *Buffer) Reset() {
	// 注意：在 Split 场景下，多个 Buffer 可能引用同一个 Chunk
	// 这里简单的 Put 回池子仅适用于你确定该 Buffer 独占这些 Chunk 的情况
	// 如果需要严谨，需要引入引用计数。但在 gnet 解析完即销毁的场景，
	// 通常在处理完最后一个 Split 块后统一释放即可。
	if b.hasSmall && b.small != nil {
		putSmallChunk(b.small)
	}
	b.small = nil
	for i := range b.big {
		putBigChunk(b.big[i])
	}
	b.big = b.big[:0]
	b.length = 0
	b.firstPageOffset = 0
	b.hasSmall = true
	b.capacity = 0
	b.version++
}

//go:inline Data 返回当前 Buffer 逻辑范围内所有物理块的切片引用。这是一个零拷贝操作，返回的 []byte 直接指向内存池中的物理内存。
func (b *Buffer) Data() [][]byte {
	return dataSlices(b.hasSmall, b.small, b.big, b.firstPageOffset, b.length)
}

// Len 返回当前 Buffer 中逻辑有效的总字节数
func (b *Buffer) Len() int {
	return b.length
}

//go:inline At 返回逻辑偏移 index 处的单个字节
func (b *Buffer) At(index int) byte {
	// 1. 极简边界检查：利用 uint 一次性判定 index < 0 || index >= v.length
	if uint(index) >= uint(b.length) {
		panic("Buffer: slice index out of range")
	}
	// 2. 调用物理索引函数（确保 atByte 也是内联的）
	return atByte(b.hasSmall, b.small, b.big, b.firstPageOffset, index)
}

// Swap 交换两个 IndexedBuffer 的所有权
// 这是一个 O(1) 操作，仅交换指针和逻辑位移
func (b *Buffer) Swap(other *Buffer) {
	if b == nil || other == nil {
		return
	}

	// 1. 物理交换小页数组 (触发 256 字节拷贝)
	b.small, other.small = other.small, b.small

	// 2. 交换大页切片引用 (仅交换指针和长度/容量信息)
	b.big, other.big = other.big, b.big

	// 3. 交换状态位与长度
	b.hasSmall, other.hasSmall = other.hasSmall, b.hasSmall
	b.length, other.length = other.length, b.length
	b.firstPageOffset, other.firstPageOffset = other.firstPageOffset, b.firstPageOffset

	// 4. 【核心优化】交换预计算的物理容量
	// 确保 Swap 后，ensureCapacity 的内联检查依然准确
	b.capacity, other.capacity = other.capacity, b.capacity
	b.version++
}

//go:inline Bytes 将视图内容合并为一个连续的切片（涉及内存拷贝）建议仅在必须对接只接收 []byte 的第三方 API 时使用
func (b *Buffer) Bytes() []byte {
	return dataSlice(b.hasSmall, b.small, b.big, b.firstPageOffset, b.length)
}

// ensureCapacity 确保至少还能写 n 字节（自动扩容多页）
func (b *Buffer) ensureCapacity(n int) {
	// Fast Path: 只要物理剩余空间够，立刻返回。
	// 这里直接用 b.capacity 比较，减少加法节点的生成。
	if n <= b.capacity-(b.length+b.firstPageOffset) {
		return
	}

	// 慢速路径：交给非内联函数处理扩容、页面申请等重逻辑
	b.grow(n)
}

//go:noinline
func (b *Buffer) grow(n int) {
	// 计算当前物理占用
	physPos := b.length + b.firstPageOffset
	totalNeeded := physPos + n

	// 1. 特殊场景：首次写入且极大，直接放弃 Small 模式节省内存
	if physPos == 0 && b.small == nil && len(b.big) == 0 && b.hasSmall {
		if totalNeeded > SmallChunkSize {
			b.hasSmall = false
		}
	}

	// 2. 确保 SmallChunk 存在（如果在 Small 模式下）
	if b.hasSmall && b.small == nil {
		b.small = getSmallChunk()
		b.capacity += SmallChunkSize
	}

	// 3. 循环补充 BigChunks
	// 这里的 b.capacity 必须代表物理总容量（Small + 所有 Big）
	for b.capacity < totalNeeded {
		b.big = append(b.big, getBigChunk())
		b.capacity += ChunkSize
	}
}

func (b *Buffer) reserve() []byte {
	pLen := b.length + b.firstPageOffset

	// Fast Path 1: 还在 SmallChunk 范围内且已分配
	if b.hasSmall && b.small != nil && pLen < SmallChunkSize {
		return b.small[pLen:]
	}

	// Fast Path 2: 已经在 BigChunk 且当前页未满
	if !b.hasSmall && len(b.big) > 0 {
		innerOff := pLen & bigMask
		// 只有在当前页还有剩余空间时才内联返回
		if innerOff < ChunkSize {
			return b.big[len(b.big)-1][innerOff:]
		}
	}

	// 复杂情况：Small未分配、跨页、需分配新页等全部交给 Slow Path
	return b.reserveSlow(pLen)
}

//go:noinline
func (b *Buffer) reserveSlow(pLen int) []byte {
	if b.hasSmall && pLen < SmallChunkSize {
		if b.small == nil {
			b.small = getSmallChunk()
		}
		return b.small[pLen:]
	}

	prefix := 0
	if b.hasSmall {
		prefix = SmallChunkSize
	}

	bigPos := pLen - prefix
	idx := bigPos >> bigShift
	off := bigPos & bigMask

	// 确保页面存在（逻辑应由调用方通过 ensureCapacity 保证，这里做安全检查）
	if idx >= len(b.big) {
		return nil
	}
	return b.big[idx][off:]
}

// Reserve 预留 n 字节，返回当前页中的可写 slice。
// 若当前页剩余空间不足，会自动扩页。
func (b *Buffer) Reserve(n int) []byte {
	b.ensureCapacity(n)
	left := b.reserve()
	if len(left) > n {
		left = left[:n]
	}
	return left
}

// Advance 前进写指针 n 字节（告诉 Buffer 实际写入了多少）
func (b *Buffer) Advance(n int) {
	b.length += n
}

func (b *Buffer) WriteTo(wr io.Writer) (int64, error) {
	if b.length <= 0 {
		return 0, nil
	}

	var total int64
	remaining := b.length
	currOff := b.firstPageOffset

	// 1. 处理 SmallChunk (如果有)
	if b.hasSmall {
		actualRead := min(SmallChunkSize-currOff, remaining)
		n, err := wr.Write(b.small[currOff : currOff+actualRead])
		total += int64(n)
		if err != nil {
			return total, err
		}
		remaining -= actualRead
		currOff = 0
	}

	// 2. 优化 BigChunk 循环：固定 4KB 逻辑简化
	for i := 0; i < len(b.big) && remaining > 0; i++ {
		// 利用固定 ChunkSize 简化计算
		canRead := ChunkSize - currOff
		if canRead > remaining {
			canRead = remaining
		}

		n, err := wr.Write(b.big[i][currOff : currOff+canRead])
		total += int64(n)
		if err != nil {
			return total, err
		}

		remaining -= canRead
		currOff = 0
	}
	return total, nil
}

func (b *Buffer) IndexByte(c byte, start int) int {
	return indexByteGeneric(b.hasSmall, b.small, b.big, b.firstPageOffset, b.length, c, start)
}

// Truncate 将 Buffer 截断为前 n 字节。
//
//go:inline
func (b *Buffer) Truncate(n int) {
	if n >= b.length {
		return
	}
	if n <= 0 {
		b.Reset()
		//b.version++
		return
	}

	b.length = n

	// 物理截断点判定
	splitPos := n + b.firstPageOffset
	prefix := 0
	if b.hasSmall {
		prefix = SmallChunkSize
	}

	// 核心修正：只要当前有大页，就需要判断是否回收
	if len(b.big) > 0 {
		var keepCount int
		if splitPos > prefix {
			// 截断点在大页区域内
			bigSplitPos := splitPos - prefix
			keepCount = (bigSplitPos + bigMask) >> bigShift
		} else {
			// 截断点回到了 Small 页区域或更早，所有大页必须回收
			keepCount = 0
		}

		// 只有当需要释放页或清空切片时才进慢路径
		if keepCount < len(b.big) {
			b.truncatePhysicalSlow(keepCount, prefix)
		}
	}
	b.version++
}

//go:noinline
func (b *Buffer) truncatePhysicalSlow(keepCount int, prefix int) {
	// 1. 释放物理大页
	for i := keepCount; i < len(b.big); i++ {
		putBigChunk(b.big[i])
	}

	// 2. 更新状态
	if keepCount == 0 {
		b.big = nil
		// 即使 b.small 为 nil，只要 hasSmall 为 true，逻辑容量就是 SmallChunkSize
		if b.hasSmall {
			b.capacity = SmallChunkSize
		} else {
			b.capacity = 0
		}
	} else {
		b.big = b.big[:keepCount]
		b.capacity = prefix + keepCount*ChunkSize
	}
}
