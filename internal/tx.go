package kvdb

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unsafe"
)

// txid 表示内部事务标识符
type txid uint64

// Tx 表示数据库上的只读或读写事务
// 只读事务可用于检索键值和创建游标
// 读写事务可以创建和删除存储桶以及创建和删除键
//
// 重要提示：完成事务后必须提交或回滚事务
// 在没有更多事务使用页面之前，写入器无法回收页面
// 长时间运行的读事务可能导致数据库快速增长
type Tx struct {
	writable       bool
	managed        bool
	db             *DB
	meta           *meta
	root           Bucket
	pages          map[pgid]*page
	stats          TxStats
	commitHandlers []func()

	// WriteFlag 指定写相关方法（如 WriteTo()）的标志
	// Tx 使用指定的标志打开数据库文件来复制数据
	//
	// 默认情况下，标志未设置，这对于主要在内存中的工作负载效果很好
	// 对于比可用 RAM 大得多的数据库，将标志设置为 syscall.O_DIRECT 以避免破坏页面缓存
	WriteFlag int
}

// init 初始化事务
func (tx *Tx) init(db *DB) {
	tx.db = db
	tx.pages = nil

	// 复制元页面，因为它可能被写入器更改
	tx.meta = &meta{}
	db.meta().copy(tx.meta)

	// 复制根存储桶
	tx.root = newBucket(tx)
	tx.root.bucket = &bucket{}
	*tx.root.bucket = tx.meta.root

	// 递增事务ID并为可写事务添加页面缓存
	if tx.writable {
		tx.pages = make(map[pgid]*page)

		// 使用时间戳作为事务ID，确保单调递增
		newTxid := txid(time.Now().UnixNano())

		// 确保新的txid大于当前的txid，处理时钟回退的情况
		if newTxid <= tx.meta.txid {
			newTxid = tx.meta.txid + 1
		}

		// 保持奇偶性，第一次生成为奇数
		if tx.meta.txid == 0 {
			// 第一次，确保为奇数
			if newTxid%2 == 0 {
				newTxid++
			}
		} else {
			// 后续保持奇偶性交替
			if newTxid%2 == tx.meta.txid%2 {
				newTxid++
			}
		}

		tx.meta.txid = newTxid
	}
}

// ID 返回事务ID
func (tx *Tx) ID() int {
	return int(tx.meta.txid)
}

// DB 返回创建事务的数据库引用
func (tx *Tx) DB() *DB {
	return tx.db
}

// Size 返回此事务看到的当前数据库大小（字节）
func (tx *Tx) Size() int64 {
	return int64(tx.meta.pgid) * int64(tx.db.pageSize)
}

// Writable 返回事务是否可以执行写操作
func (tx *Tx) Writable() bool {
	return tx.writable
}

// Cursor 创建与根存储桶关联的游标
// 游标中的所有项目都将返回nil值，因为所有根存储桶键都指向存储桶
// 游标仅在事务打开时有效，事务关闭后不要使用游标
func (tx *Tx) Cursor() *Cursor {
	return tx.root.Cursor()
}

// Stats 检索当前事务统计信息的副本
func (tx *Tx) Stats() TxStats {
	return tx.stats
}

// Bucket 按名称检索存储桶
// 如果存储桶不存在则返回nil
// 存储桶实例仅在事务生命周期内有效
func (tx *Tx) Bucket(name []byte) *Bucket {
	return tx.root.Bucket(name)
}

// CreateBucket 创建新存储桶
// 如果存储桶已存在、存储桶名称为空或存储桶名称过长，则返回错误
// 存储桶实例仅在事务生命周期内有效
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	return tx.root.CreateBucket(name)
}

// CreateBucketIfNotExists 如果存储桶不存在则创建新存储桶
// 如果存储桶名称为空或存储桶名称过长，则返回错误
// 存储桶实例仅在事务生命周期内有效
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	return tx.root.CreateBucketIfNotExists(name)
}

// DeleteBucket 删除存储桶
// 如果找不到存储桶或键表示非存储桶值，则返回错误
func (tx *Tx) DeleteBucket(name []byte) error {
	return tx.root.DeleteBucket(name)
}

// ForEach 为根中的每个存储桶执行函数
// 如果提供的函数返回错误，则停止迭代并将错误返回给调用者
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	return tx.root.ForEach(func(k, v []byte) error {
		if err := fn(k, tx.root.Bucket(k)); err != nil {
			return err
		}
		return nil
	})
}

// OnCommit 添加在事务成功提交后执行的处理函数
func (tx *Tx) OnCommit(fn func()) {
	tx.commitHandlers = append(tx.commitHandlers, fn)
}

// Commit 将所有更改写入磁盘并更新元页面
// 如果发生磁盘写入错误或在只读事务上调用Commit，则返回错误
func (tx *Tx) Commit() error {
	_assert(!tx.managed, "managed tx commit not allowed")
	if tx.db == nil {
		return ErrTxClosed
	} else if !tx.writable {
		return ErrTxNotWritable
	}

	// TODO(benbjohnson): 使用向量化I/O写出脏页面

	// 重新平衡已删除的节点
	var startTime = time.Now()
	tx.root.rebalance()
	if tx.stats.Rebalance > 0 {
		tx.stats.RebalanceTime += time.Since(startTime)
	}

	// 将数据溢出到脏页面
	startTime = time.Now()
	if err := tx.root.spill(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.SpillTime += time.Since(startTime)

	// 释放旧的根存储桶
	tx.meta.root.root = tx.root.root

	opgid := tx.meta.pgid

	// 释放空闲列表并为其分配新页面
	// 这会高估空闲列表的大小但不会低估大小（这会很糟糕）
	tx.db.freelist.free(tx.meta.txid, tx.db.page(tx.meta.freelist))
	p, err := tx.allocate((tx.db.freelist.size() / tx.db.pageSize) + 1)
	if err != nil {
		tx.rollback()
		return err
	}
	if err := tx.db.freelist.write(p); err != nil {
		tx.rollback()
		return err
	}
	tx.meta.freelist = p.id

	// 如果高水位标记上移，则尝试增长数据库
	if tx.meta.pgid > opgid {
		if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
			tx.rollback()
			return err
		}
	}

	// 将脏页面写入磁盘
	startTime = time.Now()
	if err := tx.write(); err != nil {
		tx.rollback()
		return err
	}

	// 如果启用严格模式，则执行一致性检查
	// 只有第一个一致性错误会在panic中报告
	if tx.db.StrictMode {
		ch := tx.Check()
		var errs []string
		for {
			err, ok := <-ch
			if !ok {
				break
			}
			errs = append(errs, err.Error())
		}
		if len(errs) > 0 {
			panic("check fail: " + strings.Join(errs, "\n"))
		}
	}

	// 将元数据写入磁盘
	if err := tx.writeMeta(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.WriteTime += time.Since(startTime)

	// 完成事务
	tx.close()

	// 现在锁已被移除，执行提交处理程序
	for _, fn := range tx.commitHandlers {
		fn()
	}

	return nil
}

// Rollback 关闭事务并忽略所有先前的更新
// 只读事务必须回滚而不是提交
func (tx *Tx) Rollback() error {
	_assert(!tx.managed, "managed tx rollback not allowed")
	if tx.db == nil {
		return ErrTxClosed
	}
	tx.rollback()
	return nil
}

func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		tx.db.freelist.rollback(tx.meta.txid)
		tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist))
	}
	tx.close()
}

func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		// 获取空闲列表统计信息
		var freelistFreeN = tx.db.freelist.free_count()
		var freelistPendingN = tx.db.freelist.pending_count()
		var freelistAlloc = tx.db.freelist.size()

		// 移除事务引用和写锁
		tx.db.rwtx = nil
		tx.db.rwlock.Unlock()

		// 合并统计信息
		tx.db.statlock.Lock()
		tx.db.stats.FreePageN = freelistFreeN
		tx.db.stats.PendingPageN = freelistPendingN
		tx.db.stats.FreeAlloc = (freelistFreeN + freelistPendingN) * tx.db.pageSize
		tx.db.stats.FreelistInuse = freelistAlloc
		tx.db.stats.TxStats.add(&tx.stats)
		tx.db.statlock.Unlock()
	} else {
		tx.db.removeTx(tx)
	}

	// 清除所有引用
	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
}

// Copy 将整个数据库写入写入器
// 此函数为向后兼容而存在
//
// 已弃用；请使用 WriteTo()
func (tx *Tx) Copy(w io.Writer) error {
	_, err := tx.WriteTo(w)
	return err
}

// WriteTo 将整个数据库写入写入器
// 如果 err == nil，则恰好将 tx.Size() 字节写入写入器
func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	// 尝试使用 WriteFlag 打开读取器
	f, err := os.OpenFile(tx.db.path, os.O_RDONLY|tx.WriteFlag, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// 生成元页面，两个元页面使用相同的页面数据
	buf := make([]byte, tx.db.pageSize)
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = metaPageFlag
	*page.meta() = *tx.meta

	// 写入元页面0
	page.id = 0
	page.meta().checksum = page.meta().sum64()
	nn, err := w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 0 copy: %s", err)
	}

	// 写入元页面1，使用较低的事务ID
	page.id = 1
	page.meta().txid -= 1
	page.meta().checksum = page.meta().sum64()
	nn, err = w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 1 copy: %s", err)
	}

	// 跳过文件中的元页面
	if _, err := f.Seek(int64(tx.db.pageSize*2), os.SEEK_SET); err != nil {
		return n, fmt.Errorf("seek: %s", err)
	}

	// 复制数据页面
	wn, err := io.CopyN(w, f, tx.Size()-int64(tx.db.pageSize*2))
	n += wn
	if err != nil {
		return n, err
	}

	return n, f.Close()
}

// CopyFile 将整个数据库复制到给定路径的文件
// 在复制期间维护读取器事务，因此在复制进行时继续使用数据库是安全的
func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	err = tx.Copy(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Check 对此事务的数据库执行多项一致性检查
// 如果发现任何不一致，则返回错误
//
// 可以在可写事务上安全地并发运行
// 但是，对于大型数据库和具有大量子存储桶的数据库，由于缓存，这会产生高成本
// 如果在只读事务上运行，可以消除此开销，但是同时执行其他写入器事务是不安全的
func (tx *Tx) Check() <-chan error {
	ch := make(chan error)
	go tx.check(ch)
	return ch
}

func (tx *Tx) check(ch chan error) {
	// 检查是否有页面被双重释放
	freed := make(map[pgid]bool)
	all := make([]pgid, tx.db.freelist.count())
	tx.db.freelist.copyall(all)
	for _, id := range all {
		if freed[id] {
			ch <- fmt.Errorf("page %d: already freed", id)
		}
		freed[id] = true
	}

	// 跟踪每个可达页面
	reachable := make(map[pgid]*page)
	reachable[0] = tx.page(0) // meta0
	reachable[1] = tx.page(1) // meta1
	for i := uint32(0); i <= tx.page(tx.meta.freelist).overflow; i++ {
		reachable[tx.meta.freelist+pgid(i)] = tx.page(tx.meta.freelist)
	}

	// 递归检查存储桶
	tx.checkBucket(&tx.root, reachable, freed, ch)

	// 确保高水位标记以下的所有页面都是可达的或已释放的
	for i := pgid(0); i < tx.meta.pgid; i++ {
		_, isReachable := reachable[i]
		if !isReachable && !freed[i] {
			ch <- fmt.Errorf("page %d: unreachable unfreed", int(i))
		}
	}

	// 关闭通道以表示完成
	close(ch)
}

func (tx *Tx) checkBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {
	// 忽略内联存储桶
	if b.root == 0 {
		return
	}

	// 检查此存储桶使用的每个页面
	b.tx.forEachPage(b.root, 0, func(p *page, _ int) {
		if p.id > tx.meta.pgid {
			ch <- fmt.Errorf("page %d: out of bounds: %d", int(p.id), int(b.tx.meta.pgid))
		}

		// 确保每个页面只被引用一次
		for i := pgid(0); i <= pgid(p.overflow); i++ {
			var id = p.id + i
			if _, ok := reachable[id]; ok {
				ch <- fmt.Errorf("page %d: multiple references", int(id))
			}
			reachable[id] = p
		}

		// 我们应该只遇到未释放的叶子和分支页面
		if freed[p.id] {
			ch <- fmt.Errorf("page %d: reachable freed", int(p.id))
		} else if (p.flags&branchPageFlag) == 0 && (p.flags&leafPageFlag) == 0 {
			ch <- fmt.Errorf("page %d: invalid type: %s", int(p.id), p.typ())
		}
	})

	// 检查此存储桶内的每个存储桶
	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			tx.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}

// allocate 返回从给定页面开始的连续内存块
func (tx *Tx) allocate(count int) (*page, error) {
	p, err := tx.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// 保存到页面缓存
	tx.pages[p.id] = p

	// 更新统计信息
	tx.stats.PageCount++
	tx.stats.PageAlloc += count * tx.db.pageSize

	return p, nil
}

// write 将任何脏页面写入磁盘
func (tx *Tx) write() error {
	// 按ID排序页面
	pages := make(pages, 0, len(tx.pages))
	for _, p := range tx.pages {
		pages = append(pages, p)
	}
	// 提前清除页面缓存
	tx.pages = make(map[pgid]*page)
	sort.Sort(pages)

	// 按顺序将页面写入磁盘
	for _, p := range pages {
		size := (int(p.overflow) + 1) * tx.db.pageSize
		offset := int64(p.id) * int64(tx.db.pageSize)

		// 以"最大分配"大小的块写出页面
		ptr := (*[maxAllocSize]byte)(unsafe.Pointer(p))
		for {
			// 将写入限制为最大分配大小
			sz := size
			if sz > maxAllocSize-1 {
				sz = maxAllocSize - 1
			}

			// 将块写入磁盘
			buf := ptr[:sz]
			if _, err := tx.db.ops.writeAt(buf, offset); err != nil {
				return err
			}

			// 更新统计信息
			tx.stats.Write++

			// 如果已写入所有块，则退出内部循环
			size -= sz
			if size == 0 {
				break
			}

			// 否则向前移动偏移量并将指针移动到下一个块
			offset += int64(sz)
			ptr = (*[maxAllocSize]byte)(unsafe.Pointer(&ptr[sz]))
		}
	}

	// 如果在DB上设置了标志，则忽略文件同步
	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// 将小页面放回页面池
	for _, p := range pages {
		// 忽略超过1页的页面大小
		// 这些是使用make()而不是页面池分配的
		if int(p.overflow) != 0 {
			continue
		}

		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:tx.db.pageSize]

		// 参见 https://go.googlesource.com/go/+/f03c9202c43e0abb130669852082117ca50aa9b1
		for i := range buf {
			buf[i] = 0
		}
		tx.db.pagePool.Put(buf)
	}

	return nil
}

// writeMeta 将元数据写入磁盘
func (tx *Tx) writeMeta() error {
	// 为元页面创建临时缓冲区
	buf := make([]byte, tx.db.pageSize)
	p := tx.db.pageInBuffer(buf, 0)
	tx.meta.write(p)

	// 将元页面写入文件
	if _, err := tx.db.ops.writeAt(buf, int64(p.id)*int64(tx.db.pageSize)); err != nil {
		return err
	}
	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// 更新统计信息
	tx.stats.Write++

	return nil
}

// page 返回具有给定ID的页面的引用
// 如果页面已被写入，则返回临时缓冲页面
func (tx *Tx) page(id pgid) *page {
	// 首先检查脏页面
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}

	// 否则直接从mmap返回
	return tx.db.page(id)
}

// forEachPage 遍历给定页面内的每个页面并执行函数
func (tx *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := tx.page(pgid)

	// 执行函数
	fn(p, depth)

	// 递归遍历子节点
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			tx.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

// Page 返回给定页面号的页面信息
// 仅在可写事务使用时才能安全并发使用
func (tx *Tx) Page(id int) (*PageInfo, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if pgid(id) >= tx.meta.pgid {
		return nil, nil
	}

	// 构建页面信息
	p := tx.db.page(pgid(id))
	info := &PageInfo{
		ID:            id,
		Count:         int(p.count),
		OverflowCount: int(p.overflow),
	}

	// 确定类型（或是否空闲）
	if tx.db.freelist.freed(pgid(id)) {
		info.Type = "free"
	} else {
		info.Type = p.typ()
	}

	return info, nil
}

// TxStats 表示事务执行的操作统计信息
type TxStats struct {
	// 页面统计
	PageCount int // 页面分配数量
	PageAlloc int // 分配的总字节数

	// 游标统计
	CursorCount int // 创建的游标数量

	// 节点统计
	NodeCount int // 节点分配数量
	NodeDeref int // 节点解引用数量

	// 重平衡统计
	Rebalance     int           // 节点重平衡数量
	RebalanceTime time.Duration // 重平衡总时间

	// 分割/溢出统计
	Split     int           // 节点分割数量
	Spill     int           // 节点溢出数量
	SpillTime time.Duration // 溢出总时间

	// 写入统计
	Write     int           // 执行的写入数量
	WriteTime time.Duration // 写入磁盘的总时间
}

func (s *TxStats) add(other *TxStats) {
	s.PageCount += other.PageCount
	s.PageAlloc += other.PageAlloc
	s.CursorCount += other.CursorCount
	s.NodeCount += other.NodeCount
	s.NodeDeref += other.NodeDeref
	s.Rebalance += other.Rebalance
	s.RebalanceTime += other.RebalanceTime
	s.Split += other.Split
	s.Spill += other.Spill
	s.SpillTime += other.SpillTime
	s.Write += other.Write
	s.WriteTime += other.WriteTime
}

// Sub 计算并返回两组事务统计信息之间的差异
// 当在两个不同时间点获取统计信息时很有用，您需要在该时间跨度内发生的性能计数器
func (s *TxStats) Sub(other *TxStats) TxStats {
	var diff TxStats
	diff.PageCount = s.PageCount - other.PageCount
	diff.PageAlloc = s.PageAlloc - other.PageAlloc
	diff.CursorCount = s.CursorCount - other.CursorCount
	diff.NodeCount = s.NodeCount - other.NodeCount
	diff.NodeDeref = s.NodeDeref - other.NodeDeref
	diff.Rebalance = s.Rebalance - other.Rebalance
	diff.RebalanceTime = s.RebalanceTime - other.RebalanceTime
	diff.Split = s.Split - other.Split
	diff.Spill = s.Spill - other.Spill
	diff.SpillTime = s.SpillTime - other.SpillTime
	diff.Write = s.Write - other.Write
	diff.WriteTime = s.WriteTime - other.WriteTime
	return diff
}
