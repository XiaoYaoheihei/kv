package fliter_test

import (
	"testing"
	"tinydb/fliter"
)

func TestBloomFilter(t *testing.T) {
	m := int32(1000) // 设置bitmap长度
	k := int32(3)    // 设置哈希函数数量
	// 创建一个新的 BloomFilter
	bloomFilter := fliter.NewBloomFliter(m, k, &fliter.Encryptor{})

	// 测试元素是否存在
	testElement := "test_element"
	exists := bloomFilter.Exist(testElement)
	if exists {
		t.Errorf("Element '%s' should not exist in the BloomFilter initially", testElement)
	}

	// 添加元素到 BloomFilter
	bloomFilter.Set(testElement)

	// 再次测试元素是否存在
	exists = bloomFilter.Exist(testElement)
	if !exists {
		t.Errorf("Element '%s' should exist in the BloomFilter after being set", testElement)
	}

	// 测试不存在的元素
	nonExistentElement := "non_existent_element"
	exists = bloomFilter.Exist(nonExistentElement)
	if exists {
		t.Errorf("Element '%s' should not exist in the BloomFilter", nonExistentElement)
	}
}
