package memtable_test

import (
	"fmt"
	"log"
	"testing"
	"tinydb/kv"
	"tinydb/memtable"
)

func TestSortedTree(t *testing.T) {
	// 创建一个新的二叉搜索树
	tree := &memtable.Tree{}
	tree.Init()

	// 插入键值对
	_, flag := tree.Set("30", []byte("1"))
	if flag {
		log.Println("sucess")
	}
	tree.Set("25", []byte("2"))
	tree.Set("45", []byte("3"))
	tree.Set("23", []byte("4"))
	tree.Set("26", []byte("5"))
	tree.Set("17", []byte("6"))
	// 获取键值对
	value, result := tree.Search("25")
	if result == kv.Success {
		fmt.Printf("Key2: %s\n", value.Kv.Value)
	}

	// 删除键值对
	oldValue, deleted := tree.Delete("30")
	if deleted {
		fmt.Printf("30 deleted. Old Value: %s\n", oldValue.Value)
	}

	// 获取所有键值对
	values := tree.GetValue()
	for _, v := range values {
		fmt.Printf("Key: %s, Value: %s\n", v.Key, v.Value)
	}

	// 获取元素数量
	count := tree.Getcount()
	fmt.Printf("Number of elements in tree: %d\n", count)
}
