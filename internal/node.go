package kvdb

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// node 表示内存中的反序列化页面
type node struct {
	bucket     *Bucket
	isLeaf     bool
	unbalanced bool
	spilled    bool
	key        []byte
	pgid       pgid
	parent     *node
	children   nodes
	inodes     inodes
}

// root 返回此节点附加到的顶级节点
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root()
}

// minKeys 返回此节点应具有的最小inode数量
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

// size 返回序列化后节点的大小
func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
	}
	return sz
}

// sizeLessThan 如果节点小于给定大小则返回true
// 这是一个优化，避免在只需要知道是否适合某个页面大小时计算大节点
func (n *node) sizeLessThan(v int) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + len(item.key) + len(item.value)
		if sz >= v {
			return false
		}
	}
	return true
}

// pageElementSize 根据节点类型返回每个页面元素的大小
func (n *node) pageElementSize() int {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// childAt 返回给定索引处的子节点
func (n *node) childAt(index int) *node {
	if n.isLeaf {
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	return n.bucket.node(n.inodes[index].pgid, n)
}

// childIndex 返回给定子节点的索引
func (n *node) childIndex(child *node) int {
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, child.key) != -1 })
	return index
}

// numChildren 返回子节点数量
func (n *node) numChildren() int {
	return len(n.inodes)
}

// nextSibling 返回具有相同父节点的下一个节点
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// prevSibling 返回具有相同父节点的前一个节点
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// put 插入键值对
func (n *node) put(oldKey, newKey, value []byte, pgid pgid, flags uint32) {
	if pgid >= n.bucket.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", pgid, n.bucket.tx.meta.pgid))
	} else if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// 查找插入索引
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, oldKey) != -1 })

	// 如果没有完全匹配且需要插入，则添加容量并移动节点
	exact := (len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey))
	if !exact {
		n.inodes = append(n.inodes, inode{})
		copy(n.inodes[index+1:], n.inodes[index:])
	}

	inode := &n.inodes[index]
	inode.flags = flags
	inode.key = newKey
	inode.value = value
	inode.pgid = pgid
	_assert(len(inode.key) > 0, "put: zero-length inode key")
}

// del 从节点中删除键
func (n *node) del(key []byte) {
	// 查找键的索引
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })

	// 如果找不到键则退出
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		return
	}

	// 从节点中删除inode
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// 标记节点需要重新平衡
	n.unbalanced = true
}

// read 从页面初始化节点
func (n *node) read(p *page) {
	n.pgid = p.id
	n.isLeaf = ((p.flags & leafPageFlag) != 0)
	n.inodes = make(inodes, int(p.count))

	for i := 0; i < int(p.count); i++ {
		inode := &n.inodes[i]
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value()
		} else {
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
		_assert(len(inode.key) > 0, "read: zero-length inode key")
	}

	// 保存第一个键，以便在溢出时可以在父节点中找到该节点
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key
		_assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

// write 将项目写入一个或多个页面
func (n *node) write(p *page) {
	// 初始化页面
	if n.isLeaf {
		p.flags |= leafPageFlag
	} else {
		p.flags |= branchPageFlag
	}

	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))

	// 如果没有要写入的项目则停止
	if p.count == 0 {
		return
	}

	// 遍历每个项目并将其写入页面
	b := (*[maxAllocSize]byte)(unsafe.Pointer(&p.ptr))[n.pageElementSize()*len(n.inodes):]
	for i, item := range n.inodes {
		_assert(len(item.key) > 0, "write: zero-length inode key")

		// 写入页面元素
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else {
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pgid = item.pgid
			_assert(elem.pgid != p.id, "write: circular dependency occurred")
		}

		// 如果键+值的长度大于最大分配大小，则需要重新分配字节数组指针
		klen, vlen := len(item.key), len(item.value)
		if len(b) < klen+vlen {
			b = (*[maxAllocSize]byte)(unsafe.Pointer(&b[0]))[:]
		}

		// 将元素的数据写入页面末尾
		copy(b[0:], item.key)
		b = b[klen:]
		copy(b[0:], item.value)
		b = b[vlen:]
	}

	// DEBUG ONLY: n.dump()
}

// split 如果合适，将节点分解为多个较小的节点
// 这应该只从spill()函数调用
func (n *node) split(pageSize int) []*node {
	var nodes []*node

	node := n
	for {
		// 将节点分成两个
		a, b := node.splitTwo(pageSize)
		nodes = append(nodes, a)

		// 如果无法分割则退出循环
		if b == nil {
			break
		}

		// 将节点设置为b，以便在下次迭代时进行分割
		node = b
	}

	return nodes
}

// splitTwo 如果合适，将节点分解为两个较小的节点
// 这应该只从split()函数调用
func (n *node) splitTwo(pageSize int) (*node, *node) {
	// 如果页面没有至少足够两页的节点或节点可以放在单个页面中，则忽略分割
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}

	// 确定开始新节点之前的阈值
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(pageSize) * fillPercent)

	// 确定分割位置和两个页面的大小
	splitIndex, _ := n.splitIndex(threshold)

	// 将节点分成两个独立的节点
	// 如果没有父节点，则需要创建一个
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// 创建新节点并将其添加到父节点
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// 在两个节点之间分割inode
	next.inodes = n.inodes[splitIndex:]
	n.inodes = n.inodes[:splitIndex]

	// 更新统计信息
	n.bucket.tx.stats.Split++

	return n, next
}

// splitIndex 查找页面填充给定阈值的位置
// 它返回索引以及第一页的大小
// 这只能从split()调用
func (n *node) splitIndex(threshold int) (index, sz int) {
	sz = pageHeaderSize

	// 循环直到我们只有第二页所需的最小键数
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = i
		inode := n.inodes[i]
		elsize := n.pageElementSize() + len(inode.key) + len(inode.value)

		// 如果我们至少有最小键数并且添加另一个节点会超过阈值，则退出并返回
		if i >= minKeysPerPage && sz+elsize > threshold {
			break
		}

		// 将元素大小添加到总大小
		sz += elsize
	}

	return
}

// spill 将节点写入脏页面并在过程中分割节点
// 如果无法分配脏页面则返回错误
func (n *node) spill() error {
	var tx = n.bucket.tx
	if n.spilled {
		return nil
	}

	// 首先溢出子节点。子节点可以在分割合并的情况下实现兄弟节点
	// 因此我们不能使用范围循环。我们必须在每次循环迭代时检查子节点大小
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ {
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// 我们不再需要子列表，因为它只用于溢出跟踪
	n.children = nil

	// 将节点分割为适当的大小。第一个节点始终是n
	var nodes = n.split(tx.db.pageSize)
	for _, node := range nodes {
		// 如果节点不是新的，则将节点的页面添加到空闲列表
		if node.pgid > 0 {
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}

		// 为节点分配连续空间
		p, err := tx.allocate((node.size() / tx.db.pageSize) + 1)
		if err != nil {
			return err
		}

		// 写入节点
		if p.id >= tx.meta.pgid {
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}
		node.pgid = p.id
		node.write(p)
		node.spilled = true

		// 插入到父inode中
		if node.parent != nil {
			var key = node.key
			if key == nil {
				key = node.inodes[0].key
			}

			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			node.key = node.inodes[0].key
			_assert(len(node.key) > 0, "spill: zero-length node key")
		}

		// 更新统计信息
		tx.stats.Spill++
	}

	// 如果根节点分割并创建了新根，那么我们也需要溢出它
	// 我们将清除子节点以确保它不会尝试重新溢出
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		return n.parent.spill()
	}

	return nil
}

// rebalance 如果节点填充大小低于阈值或没有足够的键，则尝试将节点与兄弟节点合并
func (n *node) rebalance() {
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// 更新统计信息
	n.bucket.tx.stats.Rebalance++

	// 如果节点高于阈值（25%）并且有足够的键，则忽略
	var threshold = n.bucket.tx.db.pageSize / 4
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// 根节点有特殊处理
	if n.parent == nil {
		// 如果根节点是分支且只有一个节点，则折叠它
		if !n.isLeaf && len(n.inodes) == 1 {
			// 将根的子节点上移
			child := n.bucket.node(n.inodes[0].pgid, n)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// 重新设置所有被移动的子节点的父节点
			for _, inode := range n.inodes {
				if child, ok := n.bucket.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// 删除旧子节点
			child.parent = nil
			delete(n.bucket.nodes, child.pgid)
			child.free()
		}

		return
	}

	// 如果节点没有键，则直接删除它
	if n.numChildren() == 0 {
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
		n.parent.rebalance()
		return
	}

	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// 如果idx == 0，目标节点是右兄弟，否则是左兄弟
	var target *node
	var useNextSibling = (n.parent.childIndex(n) == 0)
	if useNextSibling {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// 如果此节点和目标节点都太小，则合并它们
	if useNextSibling {
		// 重新设置所有被移动的子节点的父节点
		for _, inode := range target.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = n
				child.parent.children = append(child.parent.children, child)
			}
		}

		// 从目标复制inode并删除目标
		n.inodes = append(n.inodes, target.inodes...)
		n.parent.del(target.key)
		n.parent.removeChild(target)
		delete(n.bucket.nodes, target.pgid)
		target.free()
	} else {
		// 重新设置所有被移动的子节点的父节点
		for _, inode := range n.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = target
				child.parent.children = append(child.parent.children, child)
			}
		}

		// 将inode复制到目标并删除节点
		target.inodes = append(target.inodes, n.inodes...)
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
	}

	// 此节点或目标节点已从父节点删除，因此重新平衡它
	n.parent.rebalance()
}

// removeChild 从内存中子节点列表中删除节点
// 这不会影响inode
func (n *node) removeChild(target *node) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

// dereference 使节点将其所有inode键/值引用复制到堆内存
// 当mmap重新分配时需要这样做，以便inode不指向过时的数据
func (n *node) dereference() {
	if n.key != nil {
		key := make([]byte, len(n.key))
		copy(key, n.key)
		n.key = key
		_assert(n.pgid == 0 || len(n.key) > 0, "dereference: zero-length node key on existing node")
	}

	for i := range n.inodes {
		inode := &n.inodes[i]

		key := make([]byte, len(inode.key))
		copy(key, inode.key)
		inode.key = key
		_assert(len(inode.key) > 0, "dereference: zero-length inode key")

		value := make([]byte, len(inode.value))
		copy(value, inode.value)
		inode.value = value
	}

	// 递归解引用子节点
	for _, child := range n.children {
		child.dereference()
	}

	// 更新统计信息
	n.bucket.tx.stats.NodeDeref++
}

// free 将节点的底层页面添加到空闲列表
func (n *node) free() {
	if n.pgid != 0 {
		n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
		n.pgid = 0
	}
}

// dump writes the contents of the node to STDERR for debugging purposes.
/*
func (n *node) dump() {
	// Write node header.
	var typ = "branch"
	if n.isLeaf {
		typ = "leaf"
	}
	warnf("[NODE %d {type=%s count=%d}]", n.pgid, typ, len(n.inodes))

	// Write out abbreviated version of each item.
	for _, item := range n.inodes {
		if n.isLeaf {
			if item.flags&bucketLeafFlag != 0 {
				bucket := (*bucket)(unsafe.Pointer(&item.value[0]))
				warnf("+L %08x -> (bucket root=%d)", trunc(item.key, 4), bucket.root)
			} else {
				warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
			}
		} else {
			warnf("+B %08x -> pgid=%d", trunc(item.key, 4), item.pgid)
		}
	}
	warn("")
}
*/

type nodes []*node

func (s nodes) Len() int      { return len(s) }
func (s nodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool {
	return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1
}

// inode 表示节点内部的内部节点
// 它可以用于指向页面中的元素或指向尚未添加到页面的元素
type inode struct {
	flags uint32
	pgid  pgid
	key   []byte
	value []byte
}

type inodes []inode
