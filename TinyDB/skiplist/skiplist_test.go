package skiplist_test

import (
	"testing"
	"tinydb/skiplist"
)

func TestSkipList(t *testing.T) {
	skipList := skiplist.Newskiplist()

	// 测试插入
	skipList.Insert("1", "One")
	skipList.Insert("2", "Two")
	skipList.Insert("3", "Three")

	// 测试获取
	node := skipList.Get("1")
	if node == nil || node.Value != "One" {
		t.Errorf("Expected 'One', got %v", node)
	}

	node = skipList.Get("2")
	if node == nil || node.Value != "Two" {
		t.Errorf("Expected 'Two', got %v", node)
	}

	node = skipList.Get("3")
	if node == nil || node.Value != "Three" {
		t.Errorf("Expected 'Three', got %v", node)
	}

	// 测试删除
	deleted := skipList.Delete("2")
	if deleted != true {
		t.Errorf("Expected true, got %v", deleted)
	}

	node = skipList.Get("2")
	if node != nil {
		t.Errorf("Expected nil for key 2 after deletion, got %v", node)
	}
}
