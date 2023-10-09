package sstable

// sstable文件的元数据
// 在每一个文件的结尾
type Meta struct {
	//版本号
	version int64
	//数据区索引地址
	dataStart int64
	//数据区长度
	dataLen int64
	//稀疏索引区地址
	indexStart int64
	//索引区长度
	indexLen int64
}
