package memtable

import (
	"fmt"
	"log"
	"sync"
	"tinydb/kv"
)

// BST二叉排序树节点
type treeNode struct {
	Kv    kv.Value
	Left  *treeNode
	Right *treeNode
}

// BST二叉排序树
type Tree struct {
	//树的根节点
	root *treeNode
	//树中的元素数量
	count int
	//读写锁
	rwlock *sync.RWMutex
}

func (tree *Tree) Init() {
	tree.rwlock = &sync.RWMutex{}
}

func (tree *Tree) Getcount() int {
	return tree.count
}

// 查找key值
func (tree *Tree) Search(key string) (*treeNode, int) {
	tree.rwlock.RLock()
	defer tree.rwlock.RUnlock()

	if tree == nil {
		log.Fatal("The tree is nil")
		return nil, kv.None
	}
	currentNode := tree.root
	//开始查找元素
	for currentNode != nil {
		if key == currentNode.Kv.Key {
			if currentNode.Kv.Delete == false {
				return currentNode, kv.Success
			} else {
				//找到的元素是删除的
				return nil, kv.Deleted
			}
		}
		//分别循环左右子树查找
		if key < currentNode.Kv.Key {
			currentNode = currentNode.Left
		} else {
			currentNode = currentNode.Right
		}
	}
	//没有找到
	return nil, kv.None
}

func (tree *Tree) insert(key string, v []byte) bool {
	tree.rwlock.Lock()
	defer tree.rwlock.Unlock()

	tmp := &treeNode{}
	tmp.Kv.Key = key
	tmp.Kv.Value = v
	tmp.Kv.Delete = false
	tmp.Left = nil
	tmp.Right = nil

	if tree.root == nil {
		tree.root = tmp
		tree.count++
		return true
	}

	currentNode := tree.root
	for currentNode != nil {
		if key < currentNode.Kv.Key {
			//左子树为空，插入到左子树
			if currentNode.Left == nil {
				currentNode.Left = tmp
				tree.count++
				return true
			}
			//循环对比
			currentNode = currentNode.Left
		} else if key > currentNode.Kv.Key {
			//右子树为空，插入到右子树
			if currentNode.Right == nil {
				currentNode.Right = tmp
				tree.count++
				return true
			}
			currentNode = currentNode.Right
		}
	}
	log.Fatal("The tree fail to insert value")
	return false
}

// 设置key值并且返回旧值
// 设置新的key值不用在外部函数search
func (tree *Tree) Set(key string, v []byte) (oldvalue kv.Value, hasold bool) {
	if tree == nil {
		log.Fatal("The tree is nil")
	}

	node, res := tree.Search(key)
	var oldkv kv.Value
	var flag bool
	//数据不存在分两种情况：内存中标记为删除；内存中确实没有数据
	if node == nil {
		//此时的数据已经被标记为删除，替换数据就好
		if res == kv.Deleted {
			oldkv = *node.Kv.Copy()
			flag = true
			node.Kv.Key = key
			node.Kv.Value = v
			node.Kv.Delete = false
		} else if res == kv.None {
			//内存表中并没有此数据，插入新数据即可
			oldkv = kv.Value{}
			flag = false
			tree.insert(key, v)
		}
	} else {
		//数据存在于内存中
		oldkv = *node.Kv.Copy()
		flag = true
		node.Kv.Key = key
		node.Kv.Value = v
	}
	return oldkv, flag
}

// 删除key并且返回旧值
// 在调用删除函数之前不用在外部函数search
func (tree *Tree) Delete(key string) (oldvalue kv.Value, hasold bool) {
	if tree == nil {
		log.Fatal("The tree is nil")
	}

	node, res := tree.Search(key)
	var oldkv kv.Value
	var flag bool
	if node == nil {
		//数据不存在分为两种情况：内存中标记为删除；内存中确实没有数据
		if res == kv.Deleted {
			//此时的数据已经被标记为删除了
			oldkv = *node.Kv.Copy()
			flag = true
		} else if res == kv.None {
			//内存中确实没有数据
			oldkv = kv.Value{}
			flag = false
		}
	} else {
		//数据存在于内存中
		oldkv = *node.Kv.Copy()
		flag = true
		node.Kv.Delete = true
	}
	return oldkv, flag
}

// 遍历获取此memtable中的所有元素
func (tree *Tree) GetValue() []kv.Value {
	tree.rwlock.RLock()
	defer tree.rwlock.RUnlock()

	fmt.Println(tree.count)
	stack := Initstack(tree.count / 2)
	//将遍历结果存放到切片中
	values := make([]kv.Value, 0)
	//使用stack模拟前序遍历
	currentNode := tree.root
	for {
		if currentNode != nil {
			stack.Push(currentNode)
			currentNode = currentNode.Left
		} else {
			popNode, success := stack.Pop()
			if !success {
				break
			}
			values = append(values, popNode.Kv)
			//应该是弹出的节点的右子树
			currentNode = popNode.Right
		}
	}
	return values
}
