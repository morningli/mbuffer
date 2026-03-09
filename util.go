package mbuffer

import "bytes"

//go:inline
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

//go:inline
func atByte(hasSmall bool, small *SmallChunk, big []*Chunk, firstPageOffset int, index int) byte {
	physicalOff := index + firstPageOffset
	if hasSmall {
		if physicalOff < SmallChunkSize {
			return small[physicalOff]
		}
		physicalOff -= SmallChunkSize
	}
	pageIdx := physicalOff >> bigShift
	innerOff := physicalOff & bigMask
	return big[pageIdx][innerOff]
}

func dataSlice(hasSmall bool, small *SmallChunk, big []*Chunk, firstPageOffset, length int) []byte {
	if length <= 0 {
		return nil
	}

	// 极简 Fast Path：只保留最核心的“单页判断”
	// 减少变量声明，直接在 if 中计算，降低 AST cost
	if hasSmall {
		if small != nil && firstPageOffset+length <= SmallChunkSize {
			return small[firstPageOffset : firstPageOffset+length]
		}
	} else if len(big) > 0 {
		if firstPageOffset+length <= ChunkSize {
			return big[0][firstPageOffset : firstPageOffset+length]
		}
	}

	// 所有跨页、分配、循环逻辑全部剥离
	return dataSliceSlow(hasSmall, small, big, firstPageOffset, length)
}

//go:noinline
func dataSliceSlow(hasSmall bool, small *SmallChunk, big []*Chunk, fpo, length int) []byte {
	res := make([]byte, length)
	rem := length
	currOff := fpo
	destOff := 0

	if hasSmall && small != nil {
		canRead := SmallChunkSize - currOff
		if canRead > rem {
			canRead = rem
		}
		copy(res[destOff:], small[currOff:currOff+canRead])
		rem -= canRead
		destOff += canRead
		currOff = 0
	}

	for i := 0; i < len(big) && rem > 0; i++ {
		canRead := ChunkSize - currOff
		if canRead > rem {
			canRead = rem
		}
		copy(res[destOff:], big[i][currOff:currOff+canRead])
		rem -= canRead
		destOff += canRead
		currOff = 0
	}
	return res
}

func dataSlices(hasSmall bool, small *SmallChunk, big []*Chunk, firstPageOffset, length int) [][]byte {
	if length <= 0 {
		return nil
	}

	remaining := length
	currOff := firstPageOffset

	// --- 核心优化：精确计算需要的切片数量 ---
	needed := 0
	tmpRemaining := length
	tmpOff := firstPageOffset

	if hasSmall {
		canRead := SmallChunkSize - tmpOff
		actualRead := min(canRead, tmpRemaining)
		needed++
		tmpRemaining -= actualRead
		tmpOff = 0
	}

	if tmpRemaining > 0 {
		// 计算跨越了多少个 Big Page
		// 第一页能读多少
		firstBigCanRead := ChunkSize - tmpOff
		if tmpRemaining <= firstBigCanRead {
			needed++
		} else {
			// 剩余长度 / 每页长度，向上取整
			needed += 1 + (tmpRemaining-firstBigCanRead+ChunkSize-1)/ChunkSize
		}
	}

	// 此时 make 的 capacity 是精确的，且对于小包（1-2页），needed 会很小
	// 编译器更容易对这种小规模分配进行优化
	result := make([][]byte, 0, needed)

	// --- 后续逻辑保持不变，确保正确性 ---
	if hasSmall {
		canRead := SmallChunkSize - currOff
		actualRead := min(canRead, remaining)
		result = append(result, small[currOff:currOff+actualRead])
		remaining -= actualRead
		currOff = 0
	}

	for i := 0; i < len(big) && remaining > 0; i++ {
		start := currOff
		canRead := ChunkSize - start
		actualRead := min(canRead, remaining)
		result = append(result, big[i][start:start+actualRead])
		remaining -= actualRead
		currOff = 0
	}
	return result
}

func sliceFromPhysical(hasSmall bool, small *SmallChunk, big []*Chunk, physicalStart, length int) BufferView {
	// 1. 处理 Small 区域逻辑
	if hasSmall && physicalStart < SmallChunkSize {
		return BufferView{
			hasSmall:        true,
			small:           small,
			big:             big,
			length:          length,
			firstPageOffset: physicalStart,
		}
	}

	// 2. 统一处理 Big 区域逻辑 (合并 hasSmall 为 true/false 的 Big 偏移计算)
	offset := physicalStart
	if hasSmall {
		offset -= SmallChunkSize
	}

	// 提前计算索引，减少 BufferView 初始化时的逻辑权重
	idx := offset >> bigShift
	return BufferView{
		hasSmall:        false,
		small:           nil,
		big:             big[idx:], // 仅 Slice Header 拷贝，0 分配
		length:          length,
		firstPageOffset: offset & bigMask,
	}
}

// makeSuffixFirstPage allocates a new first page and copies suffix bytes into the END part of that page.
// It never reuses the boundary page from the old buffer.
func makeSuffixFirstPage(suffix []byte) (hasSmall bool, small *SmallChunk, big []*Chunk, firstOff int) {
	if len(suffix) <= SmallChunkSize {
		ns := getSmallChunk()
		firstOff = SmallChunkSize - len(suffix)
		copy(ns[firstOff:], suffix)
		return true, ns, nil, firstOff
	}
	nb := getBigChunk()
	firstOff = ChunkSize - len(suffix)
	copy(nb[firstOff:], suffix)
	return false, nil, []*Chunk{nb}, firstOff
}

// 辅助函数，避免重复逻辑。建议内联。
func calculateCapacity(hasSmall bool, bigCount int) int {
	cap := bigCount * ChunkSize
	if hasSmall {
		cap += SmallChunkSize
	}
	return cap
}

// indexByteGeneric 抽象了物理页扫描逻辑，专为内联优化设计
func indexByteGeneric(hasSmall bool, small *SmallChunk, big []*Chunk, fpo, length int, c byte, start int) int {
	// 1. 唯一边界检查
	if uint(start) >= uint(length) {
		return -1
	}

	pLen := fpo + start
	var page []byte

	// 2. Fast Path: 定位当前起始页 (内联友好)
	if hasSmall && pLen < SmallChunkSize {
		page = small[pLen:SmallChunkSize]
	} else {
		offset := pLen
		if hasSmall {
			offset -= SmallChunkSize
		}
		idx := offset >> bigShift
		// 注意：调用方需保证 big 索引安全
		page = big[idx][offset&bigMask : ChunkSize]
	}

	// 3. 计算本页可读长度
	remaining := length - start
	canRead := len(page)
	if canRead > remaining {
		canRead = remaining
	}

	// 4. 调用汇编优化的 IndexByte
	res := bytes.IndexByte(page[:canRead], c)
	if res >= 0 {
		return start + res
	}

	// 5. 跨页处理：仅当数据未读完时进入 Slow Path (非内联)
	if remaining > canRead {
		return indexByteSlow(hasSmall, small, big, fpo, length, c, start+canRead)
	}
	return -1
}

//go:noinline
func indexByteSlow(hasSmall bool, small *SmallChunk, big []*Chunk, fpo, length int, c byte, start int) int {
	curr := start

	// 1. 如果起点在 SmallChunk 且没找完，先处理 SmallChunk（防御性逻辑）
	if hasSmall && (fpo+curr) < SmallChunkSize {
		canRead := SmallChunkSize - (fpo + curr)
		remaining := length - curr
		actual := canRead
		if actual > remaining {
			actual = remaining
		}

		idx := bytes.IndexByte(small[fpo+curr:fpo+curr+actual], c)
		if idx >= 0 {
			return curr + idx
		}
		curr += actual
	}

	// 2. 遍历后续所有 BigChunks
	prefix := 0
	if hasSmall {
		prefix = SmallChunkSize
	}

	for curr < length {
		// 计算物理坐标
		bigPos := (fpo + curr) - prefix
		pageIdx := bigPos >> bigShift
		innerOff := bigPos & bigMask

		// 安全检查：如果索引越界（逻辑错误），立即退出
		if pageIdx >= len(big) {
			break
		}

		page := big[pageIdx]
		canRead := ChunkSize - innerOff
		remaining := length - curr
		actual := canRead
		if actual > remaining {
			actual = remaining
		}

		// 在当前 BigChunk 页面内进行汇编级扫描
		idx := bytes.IndexByte(page[innerOff:innerOff+actual], c)
		if idx >= 0 {
			return curr + idx
		}

		// 步进到下一页
		curr += actual
	}

	return -1
}

// parseIntGeneric 从物理坐标开始解析连续的数字。
// 专为内联设计，不处理负号，负号由调用方通过 At(0) 判断。
//
//go:inline
func parseIntGeneric(hasSmall bool, small *SmallChunk, big []*Chunk, fpo, length int) (int, error) {
	if length <= 0 {
		return 0, ErrInvalidInteger
	}

	n := 0
	for i := 0; i < length; i++ {
		// 直接内联 atByte 逻辑或使用 At() 的展开逻辑
		physPos := fpo + i
		var c byte
		if hasSmall && physPos < SmallChunkSize {
			c = small[physPos]
		} else {
			off := physPos
			if hasSmall {
				off -= SmallChunkSize
			}
			c = big[off>>bigShift][off&bigMask]
		}

		if c < '0' || c > '9' {
			return 0, ErrInvalidInteger
		}
		n = n*10 + int(c-'0')
	}
	return n, nil
}
