package sstable

// 元素定位，存储在稀疏索引区中，表示一个元素的起始位置和长度
type Position struct {
	//数据部分的起始索引
	Start int64
	//长度
	Len int64
	//是否已经被删除
	Deleted bool
}
