package kvdb

import (
	"fmt"
	"sort"
	"unsafe"
)

// freelist 表示所有可供分配的页面列表
// 它还跟踪已释放但仍被打开事务使用的页面
type freelist struct {
	ids     []pgid          // 所有空闲和可用的空闲页面 ID
	pending map[txid][]pgid // 按事务映射的即将空闲的页面 ID
	cache   map[pgid]bool   // 所有空闲和待处理页面 ID 的快速查找
}

// newFreelist 返回一个空的、已初始化的空闲列表
func newFreelist() *freelist {
	return &freelist{
		pending: make(map[txid][]pgid),
		cache:   make(map[pgid]bool),
	}
}

// size 返回序列化后页面的大小
func (f *freelist) size() int {
	n := f.count()
	if n >= 0xFFFF {
		// 第一个元素将用于存储计数。参见 freelist.write
		n++
	}
	return pageHeaderSize + (int(unsafe.Sizeof(pgid(0))) * n)
}

// count 返回空闲列表上的页面计数
func (f *freelist) count() int {
	return f.free_count() + f.pending_count()
}

// free_count 返回空闲页面计数
func (f *freelist) free_count() int {
	return len(f.ids)
}

// pending_count 返回待处理页面计数
func (f *freelist) pending_count() int {
	var count int
	for _, list := range f.pending {
		count += len(list)
	}
	return count
}

// copyall 将所有空闲 ID 和所有待处理 ID 的列表复制到 dst 中的一个排序列表
// f.count 返回 dst 所需的最小长度
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	for _, list := range f.pending {
		m = append(m, list...)
	}
	sort.Sort(m)
	mergepgids(dst, f.ids, m)
}

// allocate 返回给定大小的连续页面列表的起始页面 ID
// 如果找不到连续块则返回 0
func (f *freelist) allocate(n int) pgid {
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd pgid
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// 如果不连续则重置初始页面
		if previd == 0 || id-previd != 1 {
			initial = id
		}

		// 如果找到连续块则删除它并返回
		if (id-initial)+1 == pgid(n) {
			// 如果从开头分配则走快速路径，直接调整现有切片
			// 这会暂时使用额外内存，但 free() 中的 append() 会根据需要重新分配切片
			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n]
			}

			// 从空闲缓存中删除
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i)
			}

			return initial
		}

		previd = id
	}
	return 0
}

// free 为给定事务 ID 释放页面及其溢出
// 如果页面已经空闲则会发生 panic
func (f *freelist) free(txid txid, p *page) {
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// 释放页面及其所有溢出页面
	var ids = f.pending[txid]
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {
		// 验证页面尚未空闲
		if f.cache[id] {
			panic(fmt.Sprintf("page %d already freed", id))
		}

		// 添加到空闲列表和缓存
		ids = append(ids, id)
		f.cache[id] = true
	}
	f.pending[txid] = ids
}

// release 将事务 ID（或更早）的所有页面 ID 移动到空闲列表
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, ids := range f.pending {
		if tid <= txid {
			// 将事务的待处理页面移动到可用空闲列表
			// 不要从缓存中删除，因为页面仍然空闲
			m = append(m, ids...)
			delete(f.pending, tid)
		}
	}
	sort.Sort(m)
	f.ids = pgids(f.ids).merge(m)
}

// rollback 从给定的待处理事务中删除页面
func (f *freelist) rollback(txid txid) {
	// 从缓存中删除页面 ID
	for _, id := range f.pending[txid] {
		delete(f.cache, id)
	}

	// 从待处理列表中删除页面
	delete(f.pending, txid)
}

// freed 返回给定页面是否在空闲列表中
func (f *freelist) freed(pgid pgid) bool {
	return f.cache[pgid]
}

// read 从空闲列表页面初始化空闲列表
func (f *freelist) read(p *page) {
	// 如果 page.count 是最大 uint16 值 (64k)，则被视为溢出，
	// 空闲列表的大小存储为第一个元素
	idx, count := 0, int(p.count)
	if count == 0xFFFF {
		idx = 1
		count = int(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0])
	}

	// 从空闲列表复制页面 ID 列表
	if count == 0 {
		f.ids = nil
	} else {
		ids := ((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[idx:count]
		f.ids = make([]pgid, len(ids))
		copy(f.ids, ids)

		// 确保它们已排序
		sort.Sort(pgids(f.ids))
	}

	// 重建页面缓存
	f.reindex()
}

// write 将页面 ID 写入空闲列表页面
// 所有空闲和待处理 ID 都保存到磁盘，因为在程序崩溃时，所有待处理 ID 将变为空闲
func (f *freelist) write(p *page) error {
	// 合并旧的空闲 pgid 和等待打开事务的 pgid

	// 更新头部标志
	p.flags |= freelistPageFlag

	// page.count 只能容纳最多 64k 个元素，如果溢出该数字，
	// 则通过将大小放在第一个元素中来处理
	lenids := f.count()
	if lenids == 0 {
		p.count = uint16(lenids)
	} else if lenids < 0xFFFF {
		p.count = uint16(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[:lenids])
	} else {
		p.count = 0xFFFF
		((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[0] = pgid(lenids)
		f.copyall(((*[maxAllocSize]pgid)(unsafe.Pointer(&p.ptr)))[1 : lenids+1])
	}

	return nil
}

// reload 从空闲列表页面读取空闲列表并过滤掉待处理项目
func (f *freelist) reload(p *page) {
	f.read(p)

	// 构建待处理页面的缓存
	pcache := make(map[pgid]bool)
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			pcache[pendingID] = true
		}
	}

	// 检查每个空闲页面是否待处理删除
	var a []pgid
	for _, id := range f.ids {
		if !pcache[id] {
			a = append(a, id)
		}
	}
	f.ids = a

	// 重新构建可用页面的缓存
	f.reindex()
}

// reindex 重建空闲缓存
func (f *freelist) reindex() {
	f.cache = make(map[pgid]bool, len(f.ids))
	for _, id := range f.ids {
		f.cache[id] = true
	}
	for _, pendingIDs := range f.pending {
		for _, pendingID := range pendingIDs {
			f.cache[pendingID] = true
		}
	}
}
