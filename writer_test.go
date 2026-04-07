package mbuffer

import (
	"bytes"
	"testing"
)

// 1. 验证 Small 刚好写满时，Sync 是否正确进位到 Big[0] 且处于 nil 状态
func TestWriter_Sync_SmallBoundary(t *testing.T) {
	// initBuffer 写入 256 字节，刚好填满 Small
	b := initBuffer(t, true, 0, SmallChunkSize)

	// NewWriter 内部调用 Sync
	w := b.NewWriter()

	// 验证逻辑位置：应指向 Big 数组的第 0 页，偏移为 0
	if w.pageIdx != 0 || w.activeOff != 0 {
		t.Fatalf("Small boundary error: want idx 0 off 0, got %d:%d", w.pageIdx, w.activeOff)
	}

	// 此时 b.big 尚未分配，activePage 必须为 nil
	if w.activePage != nil {
		t.Errorf("activePage should be nil at boundary, got slice of len %d", len(w.activePage))
	}

	// 验证自愈：写入 1 字节
	err := w.WriteByte('K')
	if err != nil {
		t.Fatal(err)
	}

	// 检查是否成功挂载到 Big[0]
	if w.pageIdx != 0 || w.activeOff != 1 || w.activePage == nil {
		t.Errorf("After recovery: idx %d, off %d, pageNil %v", w.pageIdx, w.activeOff, w.activePage == nil)
	}
}

// 2. 验证 Big 刚好写满一页时，Sync 是否进位到下一页
func TestWriter_Sync_BigBoundary(t *testing.T) {
	// 写入 Small(256) + Big(4096) = 4352 字节
	fullLen := SmallChunkSize + ChunkSize
	b := initBuffer(t, true, 0, fullLen)

	w := b.NewWriter()

	// 验证定位：逻辑位置应指向 Big[1] 的开头
	if w.pageIdx != 1 || w.activeOff != 0 {
		t.Fatalf("Big boundary error: want idx 1 off 0, got %d:%d", w.pageIdx, w.activeOff)
	}

	// 此时 Big[1] 尚未分配
	if w.activePage != nil {
		t.Error("activePage should be nil before allocation")
	}

	// 批量写入测试：再写 10 字节
	_, err := w.Write([]byte("0123456789"))
	if err != nil {
		t.Fatal(err)
	}

	if w.pageIdx != 1 || w.activeOff != 10 || w.activePage == nil {
		t.Errorf("Big recovery error: idx %d, off %d", w.pageIdx, w.activeOff)
	}
}

// 3. 验证从 Small 中间开始写入，跨越边界到 Big 的全路径
func TestWriter_Write_CrossSmallToBig(t *testing.T) {
	// 初始在 Small 留 10 字节空间
	b := initBuffer(t, true, 0, SmallChunkSize-10)
	w := b.NewWriter()

	// 写入 20 字节：10 字节填满 Small，10 字节进入 Big[0]
	data := []byte("0123456789abcdefghij")
	n, err := w.Write(data)
	if err != nil || n != 20 {
		t.Fatalf("Cross write failed: %v", err)
	}

	if w.pageIdx != 0 || w.activeOff != 10 {
		t.Errorf("Final state error: idx %d, off %d", w.pageIdx, w.activeOff)
	}

	// 验证 pageMax 从 Small 切换到了 Big 的尺寸
	if w.pageMax != ChunkSize {
		t.Errorf("pageMax should be %d, got %d", ChunkSize, w.pageMax)
	}
}

// 4. 验证 CopyN 处理多页跳转的稳定性
func TestWriter_CopyN_MultiPage(t *testing.T) {
	b := initBuffer(t, false, 0, 0) // 纯 Big 模式，从 0 开始
	w := b.NewWriter()

	// 构造超过两页的数据 (4096 * 2 + 100)
	total := ChunkSize*2 + 100
	source := make([]byte, total)
	source[total-1] = 0xFF // 最后一个字节标记

	reader := bytes.NewReader(source)

	err := w.CopyN(reader, total)
	if err != nil {
		t.Fatal(err)
	}

	// 验证最终位置
	if w.pageIdx != 2 || w.activeOff != 100 {
		t.Errorf("CopyN multi-page error: idx %d, off %d", w.pageIdx, w.activeOff)
	}

	// 验证最后一个字节是否写入成功
	if b.big[2][99] != 0xFF {
		t.Error("Data integrity lost during multi-page CopyN")
	}
}

// 5. 验证 Sync 的幂等性与版本校验
func TestWriter_Sync_IdempotentAndVersion(t *testing.T) {
	b := initBuffer(t, true, 0, 100)
	w := b.NewWriter()

	// 记录当前状态
	off := w.activeOff
	idx := w.pageIdx

	// 多次调用 Sync
	w.Sync()
	w.Sync()

	if w.activeOff != off || w.pageIdx != idx {
		t.Error("Sync should not change state if length hasn't changed")
	}

	// 模拟 Buffer 版本变更（例如被其他 Writer 写入过）
	b.version++

	// 再次调用应该 Panic (取决于你的具体设计，这里按代码中 panic 逻辑测试)
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic on version mismatch, but didn't")
		}
	}()
	w.WriteByte('x')
}
