package kv

import (
	"encoding/json"
)

// 查找的结果
type SearchResult int

const (
	//表示没有找到
	None = iota
	//表示已经被删除
	Deleted
	//表示查找成功
	Success
)

// Value表示一个kv，作为k-v数据库，必须可以存储任何数据
type Value struct {
	Key    string
	Value  []byte
	Delete bool
}

// 二进制数据反序列化成Value
func Decode(data []byte) (Value, error) {
	var v Value
	err := json.Unmarshal(data, &v)
	return v, err
}

// Value数据序列化成二进制
func Encode(v Value) ([]byte, error) {
	return json.Marshal(v)
}

// 拷贝一份值
func (v *Value) Copy() *Value {
	return &Value{
		Key:    v.Key,
		Value:  v.Value,
		Delete: v.Delete,
	}
}
