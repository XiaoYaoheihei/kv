package skiplist

import (
	"math/rand"
	"sync"
	"time"
	"tinydb/kv"
)

const Maxlevel = 32
const p = 0.5

// 连接节点的定义
type SkipNode struct {
	Key   string
	Value string
	//当前节点向后的指针数组,数组的长度为层高
	next []*SkipNode
}

// 链表的结构定义
type SkipList struct {
	//整个节点的header
	header *SkipNode
	//原始链表的长度，表头节点不计入
	length int
	//最高的节点的层数
	height int
	mutex  *sync.RWMutex
}

func Newnode(level int, key string, value string) *SkipNode {
	node := new(SkipNode)
	node.Key = key
	node.Value = value
	node.next = make([]*SkipNode, level)
	return node
}

func Newskiplist() *SkipList {
	return &SkipList{
		header: Newnode(Maxlevel, "", ""),
		length: 0,
		height: 1,
		mutex:  new(sync.RWMutex),
	}
}

// 插入一个元素
// 1.查找到需要插入的位置，并获取相应的前驱节点
// 2.构造新的节点，并通过概率函数计算出节点的层数level
// 3.将新节点插入到第0层和第level-1层的链表中
func (list *SkipList) Insert(key string, value string) bool {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	update := make([]*SkipNode, Maxlevel)

	prev := list.header
	var next *SkipNode
	for i := list.height - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && next.Key < key {
			prev = next
			next = prev.next[i]
		}

		//目前不支持插入相同的元素
		if next != nil && next.Value == value {
			return false
		}
		update[i] = prev
	}

	level := list.randomLevel()
	node := Newnode(level, key, value)
	if level > list.height {
		list.height = level
	}

	for i := 0; i < level; i++ {
		//说明新节点层数超过了跳表当前的最高层数，此时将头节点对应层数的后继节点设置为新节点
		if update[i] == nil {
			list.header.next[i] = node
			continue
		}
		node.next[i] = update[i].next[i]
		update[i].next[i] = node
	}
	list.length++
	return true
}

// 跳表的查询
func (list *SkipList) Get(key string) *SkipNode {
	list.mutex.RLock()
	defer list.mutex.RUnlock()

	pre := list.header
	var node *SkipNode
	var next *SkipNode
	for i := list.height - 1; i >= 0; i-- {
		next = pre.next[i]
		for next != nil && next.Key < key {
			pre = next
			next = pre.next[i]
		}
		if next != nil && next.Key == key {
			node = next
			break
		}
	}

	return node
}

// 随机层数的生成
func (list *SkipList) randomLevel() int {
	level := 1
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	for r.Float64() < p && level < Maxlevel {
		level++
	}
	return level
}

// 删除节点
// 获取相应的前驱节点
// 调整前驱节点的next指针
func (list *SkipList) Delete(key string) interface{} {
	list.mutex.Lock()
	defer list.mutex.Unlock()

	prev := list.header
	var next *SkipNode
	var node *SkipNode
	update := make([]*SkipNode, list.height)
	for i := list.height - 1; i >= 0; i-- {
		next = prev.next[i]
		for next != nil && next.Key < key {
			prev = next
			next = prev.next[i]
		}
		update[i] = prev
		//判断当前的节点是不是需要删除的节点
		if next != nil && next.Key == key {
			node = next
		}
	}

	if node == nil {
		return false
	}

	//调整所有前驱节点的next指针并且调整当前已经找到节点的后续指针
	for i, v := range node.next {
		update[i].next[i] = v
		node.next[i] = nil
	}

	//重定向跳表高度
	for i := 0; i < len(list.header.next); i++ {
		if list.header.next == nil {
			list.height = i
			break
		}
	}
	list.length--
	return true
}

// 遍历获取skiplist中的每一个元素
// 当前直接把元素放入切片中，删除元素的还没有标记为删除
func (list *SkipList) GetValues() []kv.Value {
	list.mutex.RLock()
	defer list.mutex.RUnlock()

	//将遍历结果存放在切片中
	values := make([]kv.Value, 0)
	node := list.header
	for node != nil {
		tmp := node.Value
		values = append(values, tmp)
		node = node.next[0]
	}
	return values
}
