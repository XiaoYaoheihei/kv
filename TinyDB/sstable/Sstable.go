package sstable

import (
	"log"
	"os"
	"sync"
	"tinydb/kv"
)

// 一个sstable对象
type SSTable struct {
	//每一个sstable对象对应的文件fd
	file     *os.File
	filepath string
	//元数据
	tableMeta Meta
	//稀疏索引列表
	sparseIndex map[string]Position
	//排序之后的key列表,每一个sstable都应该有一个
	//其实有无这个也无所谓，可以在初始化文件的时候填充相应的map
	//所有的sstable文件共享一个key列表也是可以的，后续可以改进
	sortIndex []string
	//sstable使用排他锁（其实就是写独占锁），感觉这里其实也可以使用读写锁
	lock sync.Locker
	//在sortIndex中找到之后，使用sparseIndex迅速定位文件中的内容
}

// 初始化sstable对象对应的文件信息
func (s *SSTable) Init(path string) {
	s.filepath = path
	s.lock = &sync.Mutex{}
	s.loadFd()
}

// 从内存中查找元素,二分查找
// 首先从内存中的key列表中查找需要的key,如果存在，找到Position,再从数据区进行加载
func (s *SSTable) SearchMem(key string) (kv.Value, kv.SearchResult) {
	s.lock.Lock()
	defer s.lock.Unlock()

	p := Position{
		Start: -1,
	}
	left := 0
	right := len(s.sortIndex) - 1
	for left <= right {
		mid := (left + right) / 2
		//在key列表中找到了对应的key值
		if s.sortIndex[mid] == key {
			//在索引区中迅速定位位置
			p = s.sparseIndex[key]
			if p.Deleted {
				return kv.Value{}, kv.Deleted
			}
			break
		} else if s.sortIndex[mid] < key {
			left = mid + 1
		} else if s.sortIndex[mid] > key {
			right = mid - 1
		}
	}

	//在此sstable文件中没有找到相应的key
	if p.Start == -1 {
		return kv.Value{}, kv.None
	}

	//从磁盘文件中查找对应的内容
	bytes := make([]byte, p.Len)
	if _, err := s.file.Seek(p.Start, 0); err != nil {
		log.Println(err)
	}
	if _, err := s.file.Read(bytes); err != nil {
		log.Println(err)
	}
	value, err := kv.Decode(bytes)
	if err != nil {
		log.Println(err)
	}
	return value, kv.Success

}
