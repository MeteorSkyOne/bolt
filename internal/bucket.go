package kvdb

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	// MaxKeySize 是键的最大长度，以字节为单位
	MaxKeySize = 32768

	// MaxValueSize 是值的最大长度，以字节为单位
	MaxValueSize = (1 << 31) - 2
)

const (
	maxUint = ^uint(0)
	minUint = 0
	maxInt  = int(^uint(0) >> 1)
	minInt  = -maxInt - 1
)

const bucketHeaderSize = int(unsafe.Sizeof(bucket{}))

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// DefaultFillPercent 是分裂页面的填充百分比
// 这个值可以通过设置 Bucket.FillPercent 来改变
const DefaultFillPercent = 0.5

// Bucket 表示数据库内部键值对的集合
type Bucket struct {
	*bucket
	tx       *Tx                // 关联的事务
	buckets  map[string]*Bucket // 子桶缓存
	page     *page              // 内联页面引用
	rootNode *node              // 根页面的物化节点
	nodes    map[pgid]*node     // 节点缓存

	// 设置节点分裂时的填充阈值。默认情况下，
	// 桶会填充到50%，但如果你知道你的写入工作负载主要是追加操作，
	// 增加这个值会很有用。
	//
	// 这个值不会在事务间持久化，所以必须在每个事务中设置。
	FillPercent float64
}

// bucket 表示桶的文件存储表示
// 这作为桶键的"值"存储。如果桶足够小，
// 那么它的根页面可以在桶头部之后内联存储在"值"中。
// 对于内联桶，"root"将为0。
type bucket struct {
	root     pgid   // 桶根级页面的页面ID
	sequence uint64 // 单调递增，由NextSequence()使用
}

// newBucket 返回一个与事务关联的新桶
func newBucket(tx *Tx) Bucket {
	var b = Bucket{tx: tx, FillPercent: DefaultFillPercent}
	if tx.writable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node)
	}
	return b
}

// Tx 返回桶的事务
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Root 返回桶的根
func (b *Bucket) Root() pgid {
	return b.root
}

// Writable 返回桶是否可写
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor 创建一个与桶关联的游标
// 游标只在事务打开期间有效
// 事务关闭后不要使用游标
func (b *Bucket) Cursor() *Cursor {
	// 更新事务统计
	b.tx.stats.CursorCount++

	// 分配并返回游标
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// Bucket 通过名称检索嵌套桶
// 如果桶不存在则返回nil
// 桶实例只在事务生命周期内有效
func (b *Bucket) Bucket(name []byte) *Bucket {
	if b.buckets != nil {
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}

	// 移动游标到键
	c := b.Cursor()
	k, v, flags := c.seek(name)

	// 如果键不存在或不是桶则返回nil
	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	// 否则创建桶并缓存它
	var child = b.openBucket(v)
	if b.buckets != nil {
		b.buckets[string(name)] = child
	}

	return child
}

// 辅助方法，将父桶中的子桶值重新解释为Bucket
func (b *Bucket) openBucket(value []byte) *Bucket {
	var child = newBucket(b.tx)

	// 如果在此架构上未对齐的加载/存储被破坏且值未对齐，
	// 则简单地克隆到对齐的字节数组
	unaligned := brokenUnaligned && uintptr(unsafe.Pointer(&value[0]))&3 != 0

	if unaligned {
		value = cloneBytes(value)
	}

	// 如果这是可写事务，则需要复制桶条目
	// 只读事务可以直接指向mmap条目
	if b.tx.writable && !unaligned {
		child.bucket = &bucket{}
		*child.bucket = *(*bucket)(unsafe.Pointer(&value[0]))
	} else {
		child.bucket = (*bucket)(unsafe.Pointer(&value[0]))
	}

	// 如果桶是内联的，保存对内联页面的引用
	if child.root == 0 {
		child.page = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}

	return &child
}

// CreateBucket 在给定键处创建新桶并返回新桶
// 如果键已存在、桶名为空或桶名太长则返回错误
// 桶实例只在事务生命周期内有效
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	// 移动游标到正确位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// 如果存在现有键则返回错误
	if bytes.Equal(key, k) {
		if (flags & bucketLeafFlag) != 0 {
			return nil, ErrBucketExists
		}
		return nil, ErrIncompatibleValue
	}

	// 创建空的内联桶
	var bucket = Bucket{
		bucket:      &bucket{},
		rootNode:    &node{isLeaf: true},
		FillPercent: DefaultFillPercent,
	}
	var value = bucket.write()

	// 插入到节点
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, bucketLeafFlag)

	// 由于内联桶不允许子桶，我们需要取消引用内联页面（如果存在）
	// 这将导致桶在事务的其余部分被视为常规的非内联桶
	b.page = nil

	return b.Bucket(key), nil
}

// CreateBucketIfNotExists 如果桶不存在则创建新桶并返回引用
// 如果桶名为空或桶名太长则返回错误
// 桶实例只在事务生命周期内有效
func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	if err == ErrBucketExists {
		return b.Bucket(key), nil
	} else if err != nil {
		return nil, err
	}
	return child, nil
}

// DeleteBucket 删除给定键处的桶
// 如果桶不存在或键表示非桶值则返回错误
func (b *Bucket) DeleteBucket(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// 移动游标到正确位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// 如果桶不存在或不是桶则返回错误
	if !bytes.Equal(key, k) {
		return ErrBucketNotFound
	} else if (flags & bucketLeafFlag) == 0 {
		return ErrIncompatibleValue
	}

	// 递归删除所有子桶
	child := b.Bucket(key)
	err := child.ForEach(func(k, v []byte) error {
		if v == nil {
			if err := child.DeleteBucket(k); err != nil {
				return fmt.Errorf("delete bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// 移除缓存副本
	delete(b.buckets, string(key))

	// 释放所有桶页面到空闲列表
	child.nodes = nil
	child.rootNode = nil
	child.free()

	// 如果有匹配的键则删除节点
	c.node().del(key)

	return nil
}

// Get 检索桶中键的值
// 如果键不存在或键是嵌套桶则返回nil值
// 返回的值只在事务生命周期内有效
func (b *Bucket) Get(key []byte) []byte {
	k, v, flags := b.Cursor().seek(key)

	// 如果这是桶则返回nil
	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	// 如果目标节点与传入的键不同则返回nil
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// Put 在桶中设置键的值
// 如果键存在则其先前值将被覆盖
// 提供的值必须在事务生命周期内保持有效
// 如果桶是从只读事务创建的、键为空、键太大或值太大则返回错误
func (b *Bucket) Put(key []byte, value []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	// 移动游标到正确位置
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// 如果存在具有桶值的现有键则返回错误
	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// 插入到节点
	key = cloneBytes(key)
	c.node().put(key, key, value, 0, 0)

	return nil
}

// Delete 从桶中移除键
// 如果键不存在则什么都不做并返回nil错误
// 如果桶是从只读事务创建的则返回错误
func (b *Bucket) Delete(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// 移动游标到正确位置
	c := b.Cursor()
	_, _, flags := c.seek(key)

	// 如果已存在桶值则返回错误
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// 如果有匹配的键则删除节点
	c.node().del(key)

	return nil
}

// Sequence 返回桶的当前整数而不递增它
func (b *Bucket) Sequence() uint64 { return b.bucket.sequence }

// SetSequence 更新桶的序列号
func (b *Bucket) SetSequence(v uint64) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// 如果根节点尚未物化则物化它，以便在提交期间保存桶
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// 设置序列
	b.bucket.sequence = v
	return nil
}

// NextSequence 返回桶的自动递增整数
func (b *Bucket) NextSequence() (uint64, error) {
	if b.tx.db == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	// 如果根节点尚未物化则物化它，以便在提交期间保存桶
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// 递增并返回序列
	b.bucket.sequence++
	return b.bucket.sequence, nil
}

// ForEach 对桶中的每个键值对执行函数
// 如果提供的函数返回错误，则停止迭代并将错误返回给调用者
// 提供的函数不得修改桶；这将导致未定义的行为
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Stat 返回桶的统计信息
func (b *Bucket) Stats() BucketStats {
	var s, subStats BucketStats
	pageSize := b.tx.db.pageSize
	s.BucketN += 1
	if b.root == 0 {
		s.InlineBucketN += 1
	}
	b.forEachPage(func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			s.KeyN += int(p.count)

			// used 统计页面使用的字节数
			used := pageHeaderSize

			if p.count != 0 {
				// 如果页面有任何元素，添加所有元素头部
				used += leafPageElementSize * int(p.count-1)

				// 添加所有元素键、值大小
				// 计算利用了最后一个元素的键/值位置等于
				// 所有先前元素的键和值大小总和的事实
				// 它还包括最后一个元素的头部
				lastElement := p.leafPageElement(p.count - 1)
				used += int(lastElement.pos + lastElement.ksize + lastElement.vsize)
			}

			if b.root == 0 {
				// 对于内联桶只更新内联统计
				s.InlineBucketInuse += used
			} else {
				// 对于非内联桶更新所有叶子统计
				s.LeafPageN++
				s.LeafInuse += used
				s.LeafOverflowN += int(p.overflow)

				// 从子桶收集统计信息
				// 通过遍历所有元素头部来实现，
				// 寻找带有bucketLeafFlag的元素
				for i := uint16(0); i < p.count; i++ {
					e := p.leafPageElement(i)
					if (e.flags & bucketLeafFlag) != 0 {
						// 对于任何桶元素，打开元素值
						// 并递归调用包含桶的Stats
						subStats.Add(b.openBucket(e.value()).Stats())
					}
				}
			}
		} else if (p.flags & branchPageFlag) != 0 {
			s.BranchPageN++
			lastElement := p.branchPageElement(p.count - 1)

			// used 统计页面使用的字节数
			// 添加头部和所有元素头部
			used := pageHeaderSize + (branchPageElementSize * int(p.count-1))

			// 添加所有键和值的大小
			// 再次使用最后一个元素的位置等于
			// 所有先前元素的键、值大小总和的事实
			used += int(lastElement.pos + lastElement.ksize)
			s.BranchInuse += used
			s.BranchOverflowN += int(p.overflow)
		}

		// 跟踪最大页面深度
		if depth+1 > s.Depth {
			s.Depth = (depth + 1)
		}
	})

	// 分配统计可以从页面计数和页面大小计算
	s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize
	s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize

	// 添加子桶的最大深度以获得总嵌套深度
	s.Depth += subStats.Depth
	// 添加所有子桶的统计
	s.Add(subStats)
	return s
}

// forEachPage 遍历桶中的每个页面，包括内联页面
func (b *Bucket) forEachPage(fn func(*page, int)) {
	// 如果有内联页面则直接使用它
	if b.page != nil {
		fn(b.page, 0)
		return
	}

	// 否则遍历页面层次结构
	b.tx.forEachPage(b.root, 0, fn)
}

// forEachPageNode 遍历桶中的每个页面（或节点）
// 这也包括内联页面
func (b *Bucket) forEachPageNode(fn func(*page, *node, int)) {
	// 如果有内联页面或根节点则直接使用它
	if b.page != nil {
		fn(b.page, nil, 0)
		return
	}
	b._forEachPageNode(b.root, 0, fn)
}

func (b *Bucket) _forEachPageNode(pgid pgid, depth int, fn func(*page, *node, int)) {
	var p, n = b.pageNode(pgid)

	// 执行函数
	fn(p, n, depth)

	// 递归遍历子节点
	if p != nil {
		if (p.flags & branchPageFlag) != 0 {
			for i := 0; i < int(p.count); i++ {
				elem := p.branchPageElement(uint16(i))
				b._forEachPageNode(elem.pgid, depth+1, fn)
			}
		}
	} else {
		if !n.isLeaf {
			for _, inode := range n.inodes {
				b._forEachPageNode(inode.pgid, depth+1, fn)
			}
		}
	}
}

// spill 将此桶的所有节点写入脏页面
func (b *Bucket) spill() error {
	// 首先溢出所有子桶
	for name, child := range b.buckets {
		// 如果子桶足够小且没有子桶，则将其内联写入父桶的页面
		// 否则像普通桶一样溢出它，并使父值成为页面的指针
		var value []byte
		if child.inlineable() {
			child.free()
			value = child.write()
		} else {
			if err := child.spill(); err != nil {
				return err
			}

			// 更新此桶中的子桶头部
			value = make([]byte, unsafe.Sizeof(bucket{}))
			var bucket = (*bucket)(unsafe.Pointer(&value[0]))
			*bucket = *child.bucket
		}

		// 如果没有物化节点则跳过写入桶
		if child.rootNode == nil {
			continue
		}

		// 更新父节点
		var c = b.Cursor()
		k, _, flags := c.seek([]byte(name))
		if !bytes.Equal([]byte(name), k) {
			panic(fmt.Sprintf("misplaced bucket header: %x -> %x", []byte(name), k))
		}
		if flags&bucketLeafFlag == 0 {
			panic(fmt.Sprintf("unexpected bucket header flag: %x", flags))
		}
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	// 如果没有物化的根节点则忽略
	if b.rootNode == nil {
		return nil
	}

	// 溢出节点
	if err := b.rootNode.spill(); err != nil {
		return err
	}
	b.rootNode = b.rootNode.root()

	// 更新此桶的根节点
	if b.rootNode.pgid >= b.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
	}
	b.root = b.rootNode.pgid

	return nil
}

// inlineable 如果桶足够小可以内联写入且不包含子桶则返回true
// 否则返回false
func (b *Bucket) inlineable() bool {
	var n = b.rootNode

	// 桶必须只包含单个叶子节点
	if n == nil || !n.isLeaf {
		return false
	}

	// 如果桶包含子桶或超过内联桶大小阈值则不可内联
	var size = pageHeaderSize
	for _, inode := range n.inodes {
		size += leafPageElementSize + len(inode.key) + len(inode.value)

		if inode.flags&bucketLeafFlag != 0 {
			return false
		} else if size > b.maxInlineBucketSize() {
			return false
		}
	}

	return true
}

// 返回桶成为内联候选的最大总大小
func (b *Bucket) maxInlineBucketSize() int {
	return b.tx.db.pageSize / 4
}

// write 分配并将桶写入字节切片
func (b *Bucket) write() []byte {
	// 分配适当的大小
	var n = b.rootNode
	var value = make([]byte, bucketHeaderSize+n.size())

	// 写入桶头部
	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	// 将字节切片转换为假页面并写入根节点
	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	n.write(p)

	return value
}

// rebalance 尝试平衡所有节点
func (b *Bucket) rebalance() {
	for _, n := range b.nodes {
		n.rebalance()
	}
	for _, child := range b.buckets {
		child.rebalance()
	}
}

// node 从页面创建节点并将其与给定父节点关联
func (b *Bucket) node(pgid pgid, parent *node) *node {
	_assert(b.nodes != nil, "nodes map expected")

	// 如果节点已创建则检索它
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// 否则创建节点并缓存它
	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
	}

	// 如果这是内联桶则使用内联页面
	var p = b.page
	if p == nil {
		p = b.tx.page(pgid)
	}

	// 将页面读入节点并缓存它
	n.read(p)
	b.nodes[pgid] = n

	// 更新统计
	b.tx.stats.NodeCount++

	return n
}

// free 递归释放桶中的所有页面
func (b *Bucket) free() {
	if b.root == 0 {
		return
	}

	var tx = b.tx
	b.forEachPageNode(func(p *page, n *node, _ int) {
		if p != nil {
			tx.db.freelist.free(tx.meta.txid, p)
		} else {
			n.free()
		}
	})
	b.root = 0
}

// dereference 移除对旧mmap的所有引用
func (b *Bucket) dereference() {
	if b.rootNode != nil {
		b.rootNode.root().dereference()
	}

	for _, child := range b.buckets {
		child.dereference()
	}
}

// pageNode 返回内存中的节点（如果存在）
// 否则返回底层页面
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	// 内联桶在其值中嵌入了假页面，所以区别对待
	// 我们将返回rootNode（如果可用）或假页面
	if b.root == 0 {
		if id != 0 {
			panic(fmt.Sprintf("inline bucket non-zero page access(2): %d != 0", id))
		}
		if b.rootNode != nil {
			return nil, b.rootNode
		}
		return b.page, nil
	}

	// 检查非内联桶的节点缓存
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}

	// 最后如果没有物化节点则从事务中查找页面
	return b.tx.page(id), nil
}

// BucketStats 记录桶使用的资源统计信息
type BucketStats struct {
	// 页面计数统计
	BranchPageN     int // 逻辑分支页面数
	BranchOverflowN int // 物理分支溢出页面数
	LeafPageN       int // 逻辑叶子页面数
	LeafOverflowN   int // 物理叶子溢出页面数

	// 树统计
	KeyN  int // 键值对数量
	Depth int // B+树层数

	// 页面大小利用率
	BranchAlloc int // 为物理分支页面分配的字节数
	BranchInuse int // 实际用于分支数据的字节数
	LeafAlloc   int // 为物理叶子页面分配的字节数
	LeafInuse   int // 实际用于叶子数据的字节数

	// 桶统计
	BucketN           int // 包括顶级桶在内的桶总数
	InlineBucketN     int // 内联桶总数
	InlineBucketInuse int // 内联桶使用的字节数（也计入LeafInuse）
}

func (s *BucketStats) Add(other BucketStats) {
	s.BranchPageN += other.BranchPageN
	s.BranchOverflowN += other.BranchOverflowN
	s.LeafPageN += other.LeafPageN
	s.LeafOverflowN += other.LeafOverflowN
	s.KeyN += other.KeyN
	if s.Depth < other.Depth {
		s.Depth = other.Depth
	}
	s.BranchAlloc += other.BranchAlloc
	s.BranchInuse += other.BranchInuse
	s.LeafAlloc += other.LeafAlloc
	s.LeafInuse += other.LeafInuse

	s.BucketN += other.BucketN
	s.InlineBucketN += other.InlineBucketN
	s.InlineBucketInuse += other.InlineBucketInuse
}

// cloneBytes 返回给定切片的副本
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
