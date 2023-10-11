package sstable

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"tinydb/config"
	"tinydb/kv"
)

// 表示每一层的sstable,使用链表进行组织
type tableNode struct {
	//表示在该层中的位置，越大表示sstable文件越新
	index int
	//指向的sstable的地址
	table *SSTable
	//下一个tableNode
	next *tableNode
}

// 管理所有sstable的树
type TableTree struct {
	//链表组成的每一层sstable文件
	levels []*tableNode
	//读写锁
	lock *sync.RWMutex
}

// 创建新的sstable
func (t *TableTree) CreateNewTable(value []kv.Value) {
	t.creatTable(value, 0)
}

// 创建新的sstable并且插入到合适的level层
func (t *TableTree) creatTable(value []kv.Value, level int) *SSTable {
	//构造数据区，分别是有序的key列表，pos区，所有的k-v数据区
	keys := make([]string, 0, len(value))
	pos := make(map[string]Position)
	//所有的二进制数据
	dataArea := make([]byte, 0)
	//遍历value切片中每一个value值
	for _, v := range value {
		data, err := kv.Encode(v)
		if err != nil {
			log.Println("Failed to encode kv: ", v.Key, err)
			continue
		}
		keys = append(keys, v.Key)
		//文件定位区
		pos[v.Key] = Position{
			Start:   int64(len(dataArea)),
			Len:     int64(len(data)),
			Deleted: v.Delete,
		}
		//编成二进制之后的数据进行添加
		dataArea = append(dataArea, data...)
	}
	sort.Strings(keys)

	//构造稀疏索引区
	indexArea, err := json.Marshal(pos)
	if err != nil {
		log.Fatal("An SSTable file cannot be created,", err)
	}
	//构造元数据区
	meta := Meta{
		version:    0,
		dataStart:  0,
		dataLen:    int64(len(dataArea)),
		indexStart: int64(0 + len(dataArea)),
		indexLen:   int64(len(indexArea)),
	}
	//生成sstable
	table := &SSTable{
		tableMeta:   meta,
		sparseIndex: pos,
		sortIndex:   keys,
		lock:        &sync.RWMutex{},
	}
	//将构造好的sstable插入到整个管理的树中
	index := t.insert(table, level)
	log.Printf("Create a new SSTable,level: %d ,index: %d\r\n", level, index)

	//通过配置文件得到数据文件所在的目录
	//构造相应的文件名，之后将数据写入到数据文件中
	con := config.GetConfig()
	filepath := con.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	table.filepath = filepath
	writeDataToFile(filepath, dataArea, indexArea, meta)

	//数据写入之后，将所有的sstable文件都打开,方便后续对文件操作
	file, err := os.OpenFile(table.filepath, os.O_RDONLY, 0666)
	if err != nil {
		log.Println(" error open file ", table.filepath)
		panic(err)
	}
	table.file = file
	return table
}

// 插入一个sstable到指定的层,返回最新的位置
func (t *TableTree) insert(table *SSTable, level int) int {
	t.lock.Lock()
	defer t.lock.Unlock()

	//每次插入的sstable都必须插入到相应层的最后面
	node := t.levels[level]
	newNode := &tableNode{
		table: table,
		next:  nil,
		index: 0,
	}

	if node == nil {
		t.levels[level] = newNode
	} else {
		for node != nil {
			if node.next == nil {
				newNode.index = node.index + 1
				node.next = newNode
			} else {
				node = node.next
			}
		}
	}
	return newNode.index
}

// 从所有的sstable表中进行查询
func (t *TableTree) SearchTree(key string) (kv.Value, kv.SearchResult) {
	t.lock.RLock()
	defer t.lock.RUnlock()

	//遍历每一层的sstable文件
	for _, node := range t.levels {
		//获取每一层的所有sstable文件列表
		tables := make([]*SSTable, 0)
		for node != nil {
			tables = append(tables, node.table)
			node = node.next
		}
		//从最后一个sstable文件开始查找相关数据
		for i := len(tables) - 1; i >= 0; i-- {
			value, res := tables[i].SearchMem(key)
			//如果在此sstable中没有找到数据，换下一个sstable文件找
			if res == kv.None {
				continue
			} else {
				//找到或已经删除，直接返回结果
				return value, res
			}
		}
	}
	//所有的sstable文件中都不包含此值
	return kv.Value{}, kv.None
}

// 获取指定level的sstable总大小
func (t *TableTree) GetLevelsize(level int) int64 {
	var size int64
	node := t.levels[level]
	for node != nil {
		size += node.table.GetDbsize()
		node = node.next
	}
	return size
}

// 获取每一层中最大的索引值，也就是最新的文件标号
func (t *TableTree) getMaxIndex(level int) int {
	node := t.levels[level]
	count := 0
	for node != nil {
		count++
		node = node.next
	}
	return count
}

// 获取该层一共有多少个sstable文件
func (t *TableTree) getCount(level int) int {
	node := t.levels[level]
	number := 0
	for node != nil {
		number++
		node = node.next
	}
	return number
}

// 获取一个db文件所在树中的级数以及所在的index值
func getLevel(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("incorrect data file name: %q", name)
	}
	return level, index, nil
}
