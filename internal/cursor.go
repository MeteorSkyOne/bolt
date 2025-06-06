package kvdb

import (
	"bytes"
	"fmt"
	"sort"
)

// Cursor 表示桶中键值对的迭代器。
// 游标在事务期间有效，返回的键值仅在事务生命周期内有效。
// 修改数据后需要重新定位游标。
type Cursor struct {
	bucket *Bucket
	stack  []elemRef
}

// Bucket 返回创建此游标的桶。
func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

// First 将游标移动到桶中的第一个项目并返回其键和值。
// 如果桶为空，则返回 nil 键和值。
// 返回的键和值仅在事务生命周期内有效。
func (c *Cursor) First() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	c.first()

	// 如果落在空页面上，则移动到下一个值
	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}

	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v

}

// Last 将游标移动到桶中的最后一个项目并返回其键和值。
// 如果桶为空，则返回 nil 键和值。
// 返回的键和值仅在事务生命周期内有效。
func (c *Cursor) Last() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	ref := elemRef{page: p, node: n}
	ref.index = ref.count() - 1
	c.stack = append(c.stack, ref)
	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Next 将游标移动到桶中的下一个项目并返回其键和值。
// 如果游标在桶的末尾，则返回 nil 键和值。
// 返回的键和值仅在事务生命周期内有效。
func (c *Cursor) Next() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.next()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Prev 将游标移动到桶中的前一个项目并返回其键和值。
// 如果游标在桶的开头，则返回 nil 键和值。
// 返回的键和值仅在事务生命周期内有效。
func (c *Cursor) Prev() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// 尝试向后移动一个元素，直到成功。
	// 当到达堆栈中每个页面的开头时，向上移动堆栈。
	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index > 0 {
			elem.index--
			break
		}
		c.stack = c.stack[:i]
	}

	// 如果到达末尾则返回 nil
	if len(c.stack) == 0 {
		return nil, nil
	}

	// 向下移动堆栈以找到此分支下最后一个叶子的最后一个元素
	c.last()
	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Seek 将游标移动到给定键并返回它。
// 如果键不存在，则使用下一个键。如果没有后续键，则返回 nil 键。
// 返回的键和值仅在事务生命周期内有效。
func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	k, v, flags := c.seek(seek)

	// 如果在页面的最后一个元素之后结束，则移动到下一个
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}

	if k == nil {
		return nil, nil
	} else if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Delete 从桶中删除游标下的当前键/值。
// 如果当前键/值是桶或事务不可写，则删除失败。
func (c *Cursor) Delete() error {
	if c.bucket.tx.db == nil {
		return ErrTxClosed
	} else if !c.bucket.Writable() {
		return ErrTxNotWritable
	}

	key, _, flags := c.keyValue()
	// 如果当前值是桶，则返回错误
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	c.node().del(key)

	return nil
}

// seek 将游标移动到给定键并返回它。
// 如果键不存在，则使用下一个键。
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	// 从根页面/节点开始遍历到正确的页面
	c.stack = c.stack[:0]
	c.search(seek, c.bucket.root)
	ref := &c.stack[len(c.stack)-1]

	// 如果游标指向页面/节点的末尾，则返回 nil
	if ref.index >= ref.count() {
		return nil, nil, 0
	}

	return c.keyValue()
}

// first 将游标移动到堆栈中最后一个页面下的第一个叶子元素
func (c *Cursor) first() {
	for {
		// 当到达叶子页面时退出
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// 继续将指向第一个元素的页面添加到堆栈
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)
		c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	}
}

// last 将游标移动到堆栈中最后一个页面下的最后一个叶子元素
func (c *Cursor) last() {
	for {
		// 当到达叶子页面时退出
		ref := &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// 继续将指向堆栈中最后一个元素的页面添加
		var pgid pgid
		if ref.node != nil {
			pgid = ref.node.inodes[ref.index].pgid
		} else {
			pgid = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgid)

		var nextRef = elemRef{page: p, node: n}
		nextRef.index = nextRef.count() - 1
		c.stack = append(c.stack, nextRef)
	}
}

// next 移动到下一个叶子元素并返回键和值。
// 如果游标在最后一个叶子元素上，则保持在那里并返回 nil。
func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		// 尝试移动一个元素，直到成功。
		// 当到达堆栈中每个页面的末尾时，向上移动堆栈。
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			if elem.index < elem.count()-1 {
				elem.index++
				break
			}
		}

		// 如果到达根页面则停止并返回。这将使游标停留在最后一个页面的最后一个元素上。
		if i == -1 {
			return nil, nil, 0
		}

		// 否则从在堆栈中停止的地方开始，找到第一个叶子页面的第一个元素
		c.stack = c.stack[:i+1]
		c.first()

		// 如果这是空页面，则重新开始并向上移动堆栈
		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}

		return c.keyValue()
	}
}

// search 递归地对给定页面/节点执行二分搜索，直到找到给定键
func (c *Cursor) search(key []byte, pgid pgid) {
	p, n := c.bucket.pageNode(pgid)
	if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 {
		panic(fmt.Sprintf("invalid page type: %d: %x", p.id, p.flags))
	}
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e)

	// 如果在叶子页面/节点上，则找到特定节点
	if e.isLeaf() {
		c.nsearch(key)
		return
	}

	if n != nil {
		c.searchNode(key, n)
		return
	}
	c.searchPage(key, p)
}

func (c *Cursor) searchNode(key []byte, n *node) {
	var exact bool
	index := sort.Search(len(n.inodes), func(i int) bool {
		ret := bytes.Compare(n.inodes[i].key, key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// 递归搜索到下一个页面
	c.search(key, n.inodes[index].pgid)
}

func (c *Cursor) searchPage(key []byte, p *page) {
	// 二分搜索正确的范围
	inodes := p.branchPageElements()

	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// 递归搜索到下一个页面
	c.search(key, inodes[index].pgid)
}

// nsearch 在堆栈顶部的叶子节点中搜索键
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1]
	p, n := e.page, e.node

	// 如果有节点则搜索其内部节点
	if n != nil {
		index := sort.Search(len(n.inodes), func(i int) bool {
			return bytes.Compare(n.inodes[i].key, key) != -1
		})
		e.index = index
		return
	}

	// 如果有页面则搜索其叶子元素
	inodes := p.leafPageElements()
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1
	})
	e.index = index
}

// keyValue 返回当前叶子元素的键和值
func (c *Cursor) keyValue() ([]byte, []byte, uint32) {
	ref := &c.stack[len(c.stack)-1]
	if ref.count() == 0 || ref.index >= ref.count() {
		return nil, nil, 0
	}

	// 从节点检索值
	if ref.node != nil {
		inode := &ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags
	}

	// 或从页面检索值
	elem := ref.page.leafPageElement(uint16(ref.index))
	return elem.key(), elem.value(), elem.flags
}

// node 返回游标当前定位的节点
func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	// 如果堆栈顶部是叶子节点，则直接返回它
	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() {
		return ref.node
	}

	// 从根开始向下遍历层次结构
	var n = c.stack[0].node
	if n == nil {
		n = c.bucket.node(c.stack[0].page.id, nil)
	}
	for _, ref := range c.stack[:len(c.stack)-1] {
		_assert(!n.isLeaf, "expected branch node")
		n = n.childAt(int(ref.index))
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

// elemRef 表示对给定页面/节点上元素的引用
type elemRef struct {
	page  *page
	node  *node
	index int
}

// isLeaf 返回引用是否指向叶子页面/节点
func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0
}

// count 返回内部节点或页面元素的数量
func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count)
}
