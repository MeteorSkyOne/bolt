package kvdb

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"
	"unsafe"
)

const maxMmapStep = 1 << 30 // 1GB

const version = 2

const magic uint32 = 0x36E0E4DF

const (
	DefaultMaxBatchSize  int = 1000
	DefaultMaxBatchDelay     = 10 * time.Millisecond
	DefaultAllocSize         = 16 * 1024 * 1024
)

// DB表示持久化到磁盘文件的桶的集合。
// 所有数据访问都通过可以从DB获取的事务执行。
// 在调用Open()之前访问DB上的所有函数都将返回ErrDatabaseNotOpen。
type DB struct {
	// for debug only
	StrictMode bool

	// for debug only
	NoSync bool

	// for debug only
	NoGrowSync bool

	MmapFlags int

	// 批处理的最大大小, 如果<=0，则禁用
	MaxBatchSize int

	// 批处理开始前的最大延迟, 如果<=0，则有效禁用批处理
	MaxBatchDelay time.Duration

	// AllocSize是数据库需要创建新页面时分配的空间量。
	// 为了在增长数据文件时分摊truncate()和fsync()的成本。
	AllocSize int

	path     string
	file     *os.File
	lockfile *os.File
	dataref  []byte // mmap的只读数据，写入会抛出SEGV
	data     *[maxMapSize]byte
	datasz   int
	filesz   int // 当前磁盘文件大小
	meta0    *meta
	meta1    *meta
	pageSize int
	opened   bool
	rwtx     *Tx
	txs      []*Tx
	freelist *freelist
	stats    Stats

	pagePool sync.Pool

	batchMu sync.Mutex
	batch   *batch

	rwlock   sync.Mutex   // 一次只允许一个写入者。
	metalock sync.Mutex   // 保护元页面访问。
	mmaplock sync.RWMutex // 在重新映射期间保护mmap访问。
	statlock sync.RWMutex // 保护统计信息访问。

	ops struct {
		writeAt func(b []byte, off int64) (n int, err error)
	}

	// 只读模式。
	// 如果为true，Update()和Begin(true)会立即返回ErrDatabaseReadOnly。
	readOnly bool
}

// Path返回当前打开的数据库文件的路径。
func (db *DB) Path() string {
	return db.path
}

// GoString返回数据库的Go字符串表示形式。
func (db *DB) GoString() string {
	return fmt.Sprintf("kvdb.DB{path:%q}", db.path)
}

// String返回数据库的字符串表示形式。
func (db *DB) String() string {
	return fmt.Sprintf("DB<%q>", db.path)
}

// Open在给定路径创建并打开一个数据库。
// 如果文件不存在，将自动创建。
// 传入nil选项将导致kvdb使用默认选项打开数据库。
func Open(path string, mode os.FileMode, options *Options) (*DB, error) {
	var db = &DB{opened: true}

	if options == nil {
		options = DefaultOptions
	}
	db.NoGrowSync = options.NoGrowSync
	db.MmapFlags = options.MmapFlags

	// 为以后的DB操作设置默认值。
	db.MaxBatchSize = DefaultMaxBatchSize
	db.MaxBatchDelay = DefaultMaxBatchDelay
	db.AllocSize = DefaultAllocSize

	flag := os.O_RDWR
	if options.ReadOnly {
		flag = os.O_RDONLY
		db.readOnly = true
	}

	// 打开数据文件并为元数据写入分离同步处理程序。
	db.path = path
	var err error
	if db.file, err = os.OpenFile(db.path, flag|os.O_CREATE, mode); err != nil {
		_ = db.close()
		return nil, err
	}

	// 锁定文件，以便其他以读写模式使用kvdb的进程不能同时使用数据库。
	// 这会导致损坏，因为两个进程会分别写入元页面和空闲页面。
	// 如果!options.ReadOnly，数据库文件将被独占锁定（只有一个进程可以获取锁）。
	// 否则（设置了options.ReadOnly），数据库文件将使用共享锁（多个进程可以同时持有锁）。
	if err := flock(db, mode, !db.readOnly, options.Timeout); err != nil {
		_ = db.close()
		return nil, err
	}

	// 测试钩子的默认值
	db.ops.writeAt = db.file.WriteAt

	// 如果数据库不存在，则初始化
	if info, err := db.file.Stat(); err != nil {
		return nil, err
	} else if info.Size() == 0 {
		// 使用元页面初始化新文件。
		if err := db.init(); err != nil {
			return nil, err
		}
	} else {
		// 读取第一个元页面以确定页面大小。
		var buf [0x1000]byte
		if _, err := db.file.ReadAt(buf[:], 0); err == nil {
			m := db.pageInBuffer(buf[:], 0).meta()
			if err := m.validate(); err != nil {
				// 如果无法读取页面大小，可以假设它与操作系统相同——因为页面大小最初就是这样选择的。
				//
				// 如果第一个页面无效，并且此操作系统使用的页面大小与创建数据库时使用的页面大小不同，那么就无法访问数据库。
				db.pageSize = os.Getpagesize()
			} else {
				db.pageSize = int(m.pageSize)
			}
		}
	}

	// 初始化页面池。
	db.pagePool = sync.Pool{
		New: func() interface{} {
			return make([]byte, db.pageSize)
		},
	}

	// 内存映射数据文件。
	if err := db.mmap(options.InitialMmapSize); err != nil {
		_ = db.close()
		return nil, err
	}

	// 读入freelist。
	db.freelist = newFreelist()
	db.freelist.read(db.page(db.meta().freelist))

	// 将数据库标记为已打开并返回。
	return db, nil
}

// mmap打开底层的内存映射文件并初始化元引用。
// minsz是新mmap的最小大小。
func (db *DB) mmap(minsz int) error {
	db.mmaplock.Lock()
	defer db.mmaplock.Unlock()

	info, err := db.file.Stat()
	if err != nil {
		return fmt.Errorf("mmap stat error: %s", err)
	} else if int(info.Size()) < db.pageSize*2 {
		return fmt.Errorf("file size too small")
	}

	// 确保大小至少为最小值。
	var size = int(info.Size())
	if size < minsz {
		size = minsz
	}
	size, err = db.mmapSize(size)
	if err != nil {
		return err
	}

	// 在取消映射之前取消所有mmap引用。
	if db.rwtx != nil {
		db.rwtx.root.dereference()
	}

	// 在继续之前取消映射现有数据。
	if err := db.munmap(); err != nil {
		return err
	}

	// 将数据文件内存映射为字节切片。
	if err := mmap(db, size); err != nil {
		return err
	}

	// 保存对元页面的引用。
	db.meta0 = db.page(0).meta()
	db.meta1 = db.page(1).meta()

	// 验证元页面。仅在两个元页面都验证失败时返回错误，
	// 因为meta0验证失败意味着它没有正确保存——但可以使用meta1恢复。反之亦然。
	err0 := db.meta0.validate()
	err1 := db.meta1.validate()
	if err0 != nil && err1 != nil {
		return err0
	}

	return nil
}

// munmap 从内存中取消映射数据文件。
func (db *DB) munmap() error {
	if err := munmap(db); err != nil {
		return fmt.Errorf("unmap error: " + err.Error())
	}
	return nil
}

// mmapSize根据数据库的当前大小确定mmap的适当大小。
// 最小大小为32KB，并以2倍增长，直到达到1GB。
// 如果新的mmap大小大于允许的最大值，则返回错误。
func (db *DB) mmapSize(size int) (int, error) {
	// 大小从32KB开始翻倍，直到1GB。
	for i := uint(15); i <= 30; i++ {
		if size <= 1<<i {
			return 1 << i, nil
		}
	}

	// 验证请求的大小不超过允许的最大值。
	if size > maxMapSize {
		return 0, fmt.Errorf("mmap too large")
	}

	// 如果大于1GB，则一次增长1GB。
	sz := int64(size)
	if remainder := sz % int64(maxMmapStep); remainder > 0 {
		sz += int64(maxMmapStep) - remainder
	}

	// 确保mmap大小是页面大小的倍数。
	// 这应该始终为true，因为以MB为单位递增。
	pageSize := int64(db.pageSize)
	if (sz % pageSize) != 0 {
		sz = ((sz / pageSize) + 1) * pageSize
	}

	// 如果超过了最大大小，则只增长到最大大小。
	if sz > maxMapSize {
		sz = maxMapSize
	}

	return int(sz), nil
}

// init创建一个新的数据库文件并初始化其元页面。
func (db *DB) init() error {
	// 将页面大小设置为操作系统页面大小。
	db.pageSize = os.Getpagesize()

	// 在缓冲区上创建两个元页面
	buf := make([]byte, db.pageSize*4)
	for i := 0; i < 2; i++ {
		p := db.pageInBuffer(buf[:], pgid(i))
		p.id = pgid(i)
		p.flags = metaPageFlag

		// 初始化元页面。
		m := p.meta()
		m.magic = magic
		m.version = version
		m.pageSize = uint32(db.pageSize)
		m.freelist = 2
		m.root = bucket{root: 3}
		m.pgid = 4
		m.txid = txid(i)
		m.checksum = m.sum64()
	}

	// 在第3页上写入一个空的freelist
	p := db.pageInBuffer(buf[:], pgid(2))
	p.id = pgid(2)
	p.flags = freelistPageFlag
	p.count = 0

	// 在第4页上写入一个空的叶子页
	p = db.pageInBuffer(buf[:], pgid(3))
	p.id = pgid(3)
	p.flags = leafPageFlag
	p.count = 0

	// 将缓冲区写入数据文件。
	if _, err := db.ops.writeAt(buf, 0); err != nil {
		return err
	}
	if err := fdatasync(db); err != nil {
		return err
	}

	return nil
}

// ForceClose 强制关闭数据库。
func (db *DB) ForceClose() error {
	return db.close()
}

// Close 释放所有数据库资源。
// 在关闭数据库之前，必须关闭所有事务。
func (db *DB) Close() error {
	db.rwlock.Lock()
	defer db.rwlock.Unlock()

	db.metalock.Lock()
	defer db.metalock.Unlock()

	db.mmaplock.RLock()
	defer db.mmaplock.RUnlock()

	return db.close()
}

func (db *DB) close() error {
	if !db.opened {
		return nil
	}

	db.opened = false

	db.freelist = nil

	// 清除ops。
	db.ops.writeAt = nil

	// 关闭mmap。
	if err := db.munmap(); err != nil {
		return err
	}

	// 关闭文件句柄。
	if db.file != nil {
		// 无需解锁只读文件。
		if !db.readOnly {
			// 解锁文件。
			if err := funlock(db); err != nil {
				log.Printf("kvdb.Close(): funlock error: %s", err)
			}
		}

		// 关闭文件描述符。
		if err := db.file.Close(); err != nil {
			return fmt.Errorf("db file close: %s", err)
		}
		db.file = nil
	}

	db.path = ""
	return nil
}

// Begin 开始一个新的事务。
// 可以同时使用多个只读事务，但一次只能使用一个写事务。
// 启动多个写事务将导致调用阻塞并序列化，直到当前写事务完成。
//
// 事务不应相互依赖。在同一个goroutine中打开读事务和写事务可能导致写程序死锁，
// 因为数据库需要定期在增长时重新mmap自身，而在读事务打开时无法执行此操作。
//
// 完成后必须关闭只读事务，否则数据库将不会回收旧页面。
func (db *DB) Begin(writable bool) (*Tx, error) {
	if writable {
		return db.beginRWTx()
	}
	return db.beginTx()
}

func (db *DB) beginTx() (*Tx, error) {
	// 在初始化事务时锁定元页面。在mmap锁之前获取元锁，
	// 因为这是写事务获取它们的顺序。
	db.metalock.Lock()

	// 获取mmap的只读锁。当mmap被重新映射时，它将获取一个写锁，
	// 因此所有事务都必须在它可以被重新映射之前完成。
	db.mmaplock.RLock()

	// 检测数据库是否打开。
	if !db.opened {
		db.mmaplock.RUnlock()
		db.metalock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// 创建与数据库关联的事务。
	t := &Tx{}
	t.init(db)

	// 跟踪事务直到它关闭。
	db.txs = append(db.txs, t)
	n := len(db.txs)

	// 解锁元页面。
	db.metalock.Unlock()

	// 更新事务统计信息。
	db.statlock.Lock()
	db.stats.TxN++
	db.stats.OpenTxN = n
	db.statlock.Unlock()

	return t, nil
}

func (db *DB) beginRWTx() (*Tx, error) {
	// 如果数据库是以Options.ReadOnly方式打开的，则返回错误。
	if db.readOnly {
		return nil, ErrDatabaseReadOnly
	}

	// 获取写入器锁。此锁在事务关闭时释放。
	// 这强制一次只有一个写入器事务。
	db.rwlock.Lock()

	// 一旦有了写入器锁，就可以锁定元页面，以便设置事务。
	db.metalock.Lock()
	defer db.metalock.Unlock()

	// 检测数据库是否打开。
	if !db.opened {
		db.rwlock.Unlock()
		return nil, ErrDatabaseNotOpen
	}

	// 创建与数据库关联的事务。
	t := &Tx{writable: true}
	t.init(db)
	db.rwtx = t

	// 释放与已关闭的只读事务关联的任何页面。
	var minid txid = 0xFFFFFFFFFFFFFFFF
	for _, t := range db.txs {
		if t.meta.txid < minid {
			minid = t.meta.txid
		}
	}
	if minid > 0 {
		db.freelist.release(minid - 1)
	}

	return t, nil
}

// removeTx 从数据库中删除一个事务。
func (db *DB) removeTx(tx *Tx) {
	// 释放mmap上的读锁。
	db.mmaplock.RUnlock()

	// 使用元锁限制对DB对象的访问。
	db.metalock.Lock()

	// 删除事务。
	for i, t := range db.txs {
		if t == tx {
			last := len(db.txs) - 1
			db.txs[i] = db.txs[last]
			db.txs[last] = nil
			db.txs = db.txs[:last]
			break
		}
	}
	n := len(db.txs)

	// 解锁元页面。
	db.metalock.Unlock()

	// 合并统计信息。
	db.statlock.Lock()
	db.stats.OpenTxN = n
	db.stats.TxStats.add(&tx.stats)
	db.statlock.Unlock()
}

// Update在读写托管事务的上下文中执行一个函数。
// 如果函数没有返回错误，则事务被提交。
// 如果返回错误，则整个事务将回滚。
// 从函数返回的任何错误或从提交返回的错误都将从Update()方法返回。
//
// 尝试在函数内手动提交或回滚将导致panic。
func (db *DB) Update(fn func(*Tx) error) error {
	t, err := db.Begin(true)
	if err != nil {
		return err
	}

	// 确保在发生panic时事务回滚。
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// 标记为托管事务，以便内部函数无法手动提交。
	t.managed = true

	// 如果函数返回错误，则回滚并返回错误。
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	return t.Commit()
}

// View在托管的只读事务的上下文中执行一个函数。
// 从函数返回的任何错误都将从View()方法返回。
//
// 尝试在函数内手动回滚将导致panic。
func (db *DB) View(fn func(*Tx) error) error {
	t, err := db.Begin(false)
	if err != nil {
		return err
	}

	// 确保在发生panic时事务回滚。
	defer func() {
		if t.db != nil {
			t.rollback()
		}
	}()

	// 标记为托管事务，以便内部函数无法手动回滚。
	t.managed = true

	// 如果函数返回错误，则将其传递出去。
	err = fn(t)
	t.managed = false
	if err != nil {
		_ = t.Rollback()
		return err
	}

	if err := t.Rollback(); err != nil {
		return err
	}

	return nil
}

// Batch将fn作为批处理的一部分调用。它的行为类似于Update，
// 除了：
//
// 1. 并发的Batch调用可以合并到单个kvdb事务中。
//
// 2. 传递给Batch的函数可能会被多次调用，无论它是否返回错误。
//
// 这意味着Batch函数的副作用必须是幂等的，并且只有在调用者看到成功返回后才能永久生效。
//
// 最大批处理大小和延迟可以分别通过DB.MaxBatchSize和DB.MaxBatchDelay进行调整。
//
// 只有在有多个goroutine调用它时，Batch才有用。
func (db *DB) Batch(fn func(*Tx) error) error {
	errCh := make(chan error, 1)

	db.batchMu.Lock()
	if (db.batch == nil) || (db.batch != nil && len(db.batch.calls) >= db.MaxBatchSize) {
		// 没有现有的批处理，或者现有的批处理已满；启动一个新的。
		db.batch = &batch{
			db: db,
		}
		db.batch.timer = time.AfterFunc(db.MaxBatchDelay, db.batch.trigger)
	}
	db.batch.calls = append(db.batch.calls, call{fn: fn, err: errCh})
	if len(db.batch.calls) >= db.MaxBatchSize {
		// 唤醒批处理，它已准备好运行
		go db.batch.trigger()
	}
	db.batchMu.Unlock()

	err := <-errCh
	if err == trySolo {
		err = db.Update(fn)
	}
	return err
}

type call struct {
	fn  func(*Tx) error
	err chan<- error
}

type batch struct {
	db    *DB
	timer *time.Timer
	start sync.Once
	calls []call
}

// trigger 运行批处理（如果尚未运行）。
func (b *batch) trigger() {
	b.start.Do(b.run)
}

// run 执行批处理中的事务，并将结果传回DB.Batch。
func (b *batch) run() {
	b.db.batchMu.Lock()
	b.timer.Stop()
	// 确保没有新工作添加到此批处理中，但不要破坏其他批处理。
	if b.db.batch == b {
		b.db.batch = nil
	}
	b.db.batchMu.Unlock()

retry:
	for len(b.calls) > 0 {
		var failIdx = -1
		err := b.db.Update(func(tx *Tx) error {
			for i, c := range b.calls {
				if err := safelyCall(c.fn, tx); err != nil {
					failIdx = i
					return err
				}
			}
			return nil
		})

		if failIdx >= 0 {
			// 从批处理中取出失败的事务。
			// 在这里缩短b.calls是安全的，因为db.batch不再指向，而且无论如何都持有互斥锁。
			c := b.calls[failIdx]
			b.calls[failIdx], b.calls = b.calls[len(b.calls)-1], b.calls[:len(b.calls)-1]
			// 告诉提交者单独重新运行它，继续处理批处理的其余部分
			c.err <- trySolo
			continue retry
		}

		// 将成功或kvdb内部错误传递给所有调用者
		for _, c := range b.calls {
			c.err <- err
		}
		break retry
	}
}

var trySolo = errors.New("batch function returned an error and should be re-run solo")

type panicked struct {
	reason interface{}
}

func (p panicked) Error() string {
	if err, ok := p.reason.(error); ok {
		return err.Error()
	}
	return fmt.Sprintf("panic: %v", p.reason)
}

func safelyCall(fn func(*Tx) error, tx *Tx) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = panicked{p}
		}
	}()
	return fn(tx)
}

// Sync 对数据库文件句柄执行fdatasync()。
func (db *DB) Sync() error { return fdatasync(db) }

// Stats 检索数据库的持续性能统计信息。
// 仅在事务关闭时更新。
func (db *DB) Stats() Stats {
	db.statlock.RLock()
	defer db.statlock.RUnlock()
	return db.stats
}

// 这是用于从C游标内部访问原始数据字节，请谨慎使用，或者根本不要使用。
func (db *DB) Info() *Info {
	return &Info{uintptr(unsafe.Pointer(&db.data[0])), db.pageSize}
}

// page根据当前页面大小从mmap中检索页面引用。
func (db *DB) page(id pgid) *page {
	pos := id * pgid(db.pageSize)
	return (*page)(unsafe.Pointer(&db.data[pos]))
}

// pageInBuffer根据当前页面大小从给定的字节数组中检索页面引用。
func (db *DB) pageInBuffer(b []byte, id pgid) *page {
	return (*page)(unsafe.Pointer(&b[id*pgid(db.pageSize)]))
}

// meta检索当前的元页面引用。
func (db *DB) meta() *meta {
	// 必须返回具有最高txid且未通过验证的元数据。
	// 否则，当数据库实际上处于一致状态时，可能会导致错误。metaA是具有较高txid的那个。
	metaA := db.meta0
	metaB := db.meta1
	if db.meta1.txid > db.meta0.txid {
		metaA = db.meta1
		metaB = db.meta0
	}

	// 如果有效，则使用较高的元页面。否则，如果有效，则回退到上一个。
	if err := metaA.validate(); err == nil {
		return metaA
	} else if err := metaB.validate(); err == nil {
		return metaB
	}

	// 不可达
	panic("kvdb.DB.meta(): invalid meta pages")
}

// allocate 返回从给定页面开始的连续内存块。
func (db *DB) allocate(count int) (*page, error) {
	// 为页面分配一个临时缓冲区。
	var buf []byte
	if count == 1 {
		buf = db.pagePool.Get().([]byte)
	} else {
		buf = make([]byte, count*db.pageSize)
	}
	p := (*page)(unsafe.Pointer(&buf[0]))
	p.overflow = uint32(count - 1)

	// 如果freelist中的页面可用，则使用它们。
	if p.id = db.freelist.allocate(count); p.id != 0 {
		return p, nil
	}

	// 如果到达了结尾，则调整mmap()的大小。
	p.id = db.rwtx.meta.pgid
	var minsz = int((p.id+pgid(count))+1) * db.pageSize
	if minsz >= db.datasz {
		if err := db.mmap(minsz); err != nil {
			return nil, fmt.Errorf("mmap allocate error: %s", err)
		}
	}

	// 移动页面id高水位线。
	db.rwtx.meta.pgid += pgid(count)

	return p, nil
}

// grow 将数据库的大小增长到给定的sz。
func (db *DB) grow(sz int) error {
	// 如果新大小小于可用文件大小，则忽略。
	if sz <= db.filesz {
		return nil
	}

	// 如果数据小于分配大小，则仅分配所需的大小。
	// 一旦超过分配大小，则分块分配。
	if db.datasz < db.AllocSize {
		sz = db.datasz
	} else {
		sz += db.AllocSize
	}

	// Truncate和fsync以确保文件大小元数据被刷新。
	if !db.NoGrowSync && !db.readOnly {
		if runtime.GOOS != "windows" {
			if err := db.file.Truncate(int64(sz)); err != nil {
				return fmt.Errorf("file resize error: %s", err)
			}
		}
		if err := db.file.Sync(); err != nil {
			return fmt.Errorf("file sync error: %s", err)
		}
	}

	db.filesz = sz
	return nil
}

func (db *DB) IsReadOnly() bool {
	return db.readOnly
}

// Options表示打开数据库时可以设置的选项。
type Options struct {
	// Timeout是获取文件锁的等待时间。
	// 当设置为零时，它将无限期等待。此选项仅在Darwin和Linux上可用。
	Timeout time.Duration

	// 在内存映射文件之前设置DB.NoGrowSync标志。
	NoGrowSync bool

	// 以只读模式打开数据库
	ReadOnly bool

	// 在内存映射文件之前设置DB.MmapFlags标志。
	MmapFlags int

	// InitialMmapSize是数据库的初始mmap大小（字节）。
	// 如果InitialMmapSize足够大以容纳数据库mmap大小，则读事务不会阻塞写事务。
	// 如果<=0，则初始映射大小为0。
	// 如果initialMmapSize小于以前的数据库大小，则它不起作用。
	InitialMmapSize int
}

// DefaultOptions表示如果将nil选项传递给Open()时使用的选项。
// 不使用超时，这将导致kvdb无限期地等待锁。
var DefaultOptions = &Options{
	Timeout:    0,
	NoGrowSync: false,
}

// Stats表示有关数据库的统计信息。
type Stats struct {
	// Freelist统计
	FreePageN     int // freelist上的空闲页面总数
	PendingPageN  int // freelist上的待处理页面总数
	FreeAlloc     int // 在空闲页面中分配的总字节数
	FreelistInuse int // freelist使用的总字节数

	// 事务统计
	TxN     int // 已启动的读事务总数
	OpenTxN int // 当前打开的读事务数

	TxStats TxStats // 全局、持续的统计信息。
}

// Sub计算并返回两组数据库统计信息之间的差异。
// 当在两个不同的时间点获取统计信息并且需要该时间段内发生的性能计数器时，这很有用。
func (s *Stats) Sub(other *Stats) Stats {
	if other == nil {
		return *s
	}
	var diff Stats
	diff.FreePageN = s.FreePageN
	diff.PendingPageN = s.PendingPageN
	diff.FreeAlloc = s.FreeAlloc
	diff.FreelistInuse = s.FreelistInuse
	diff.TxN = s.TxN - other.TxN
	diff.TxStats = s.TxStats.Sub(&other.TxStats)
	return diff
}

func (s *Stats) add(other *Stats) {
	s.TxStats.add(&other.TxStats)
}

type Info struct {
	Data     uintptr
	PageSize int
}

type meta struct {
	magic    uint32
	version  uint32
	pageSize uint32
	flags    uint32
	root     bucket
	freelist pgid
	pgid     pgid
	txid     txid
	checksum uint64
}

// validate 检查元页面的标记字节和版本，以确保它与此二进制文件匹配。
func (m *meta) validate() error {
	if m.magic != magic {
		return ErrInvalid
	} else if m.version != version {
		return ErrVersionMismatch
	} else if m.checksum != 0 && m.checksum != m.sum64() {
		return ErrChecksum
	}
	return nil
}

// copy 将一个元对象复制到另一个。
func (m *meta) copy(dest *meta) {
	*dest = *m
}

// write 将元数据写入页面。
func (m *meta) write(p *page) {
	if m.root.root >= m.pgid {
		panic(fmt.Sprintf("root bucket pgid (%d) above high water mark (%d)", m.root.root, m.pgid))
	} else if m.freelist >= m.pgid {
		panic(fmt.Sprintf("freelist pgid (%d) above high water mark (%d)", m.freelist, m.pgid))
	}

	// 页面id可以是0或1，可以通过事务ID来确定。
	p.id = pgid(m.txid % 2)
	p.flags |= metaPageFlag

	// 计算校验和。
	m.checksum = m.sum64()

	m.copy(p.meta())
}

// 为元数据生成校验和。
func (m *meta) sum64() uint64 {
	var h = fnv.New64a()
	_, _ = h.Write((*[unsafe.Offsetof(meta{}.checksum)]byte)(unsafe.Pointer(m))[:])
	return h.Sum64()
}

// _assert 如果给定条件为false，则会使用给定的格式化消息引发panic。
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}

func warn(v ...interface{})              { fmt.Fprintln(os.Stderr, v...) }
func warnf(msg string, v ...interface{}) { fmt.Fprintf(os.Stderr, msg+"\n", v...) }

func printstack() {
	stack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	fmt.Fprintln(os.Stderr, stack)
}
