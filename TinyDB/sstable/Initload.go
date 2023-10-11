package sstable

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"sync"
	"time"
	"tinydb/config"
)

var levelSize []int

// 初始化tableTree
// 1.读取目录dir中的所有level.index.db文件
// 2.将db文件的元数据和稀疏索引区数据读取到内存，并同时为每一个sstable构造一个keys数组
// 3.根据db文件名称构建tableTree
func (t *TableTree) Init(dir string) {
	log.Println("The SSTable list are being loaded")
	start := time.Now()
	defer func() {
		end := time.Since(start)
		log.Println("Loading the ", dir, ",Consumption of time : ", end)
	}()

	con := config.GetConfig()
	levelSize = make([]int, 10)
	levelSize[0] = con.Level0Size
	//初始化每一层的文件大小
	for i := 1; i < 10; i++ {
		levelSize[i] = levelSize[i-1] * 10
	}

	t.levels = make([]*tableNode, 10)
	t.lock = &sync.RWMutex{}

	dirname, err := os.OpenFile(dir, os.O_RDONLY, 0666)
	if err != nil {
		log.Println("Open dir fail")
	}
	defer dirname.Close()
	//读取目录中的所有db文件
	//Readdir读取目录的内容，返回一个有n个成员的[]FileInfo
	//n<=0，Readdir函数返回目录中剩余所有文件对象的FileInfo构成的切片
	infos, err := dirname.Readdir(-1)
	if err != nil {
		log.Println(err)
	}
	//忽略当前元素的值，所以不写
	for i := range infos {
		//如果是sstable文件的话
		//将sstable文件添加到tableTree中去
		if path.Ext(infos[i].Name()) == ".db" {
			//传入的路径带有/，需要相应的处理
			t.loadToTree(path.Join(dir, infos[i].Name()))
		}
	}
}

// 加载一个db文件到tableTree中去
func (t *TableTree) loadToTree(path string) {
	log.Println("Loading the ", path)
	start := time.Now()
	defer func() {
		end := time.Since(start)
		log.Println("Loading the ", path, ",Consumption of time : ", end)
	}()

	level, index, err := getLevel(filepath.Base(path))
	if err != nil {
		log.Println("Loading the ", path, "error")
	}

	table := &SSTable{}
	table.Init(path)
	newNode := &tableNode{
		index: index,
		table: table,
	}
	currentNode := t.levels[level]
	if currentNode == nil {
		t.levels[level] = newNode
		return
	}
	//链表节点的插入，将sstable文件插入到合适的位置
	for currentNode != nil {
		// if currentNode.next == nil {
		// 	newNode.next = currentNode.next
		// 	currentNode.next = newNode
		// 	break
		// }
		if currentNode.next == nil || (newNode.index > currentNode.index && newNode.index < currentNode.next.index) {
			newNode.next = currentNode.next
			currentNode.next = newNode
			break
		}
		currentNode = currentNode.next
	}
}

// 加载文件句柄
func (table *SSTable) loadFd() {
	if table.file == nil {
		//以只读的形式打开文件
		f, err := os.OpenFile(table.filepath, os.O_RDONLY, 0666)
		if err != nil {
			log.Println(" error open file ", table.filepath)
			panic(err)
		}
		table.file = f
	}
	//首先加载元数据
	//然后根据文件的元数据加载稀疏索引区数据到内存中
	table.loadMeta()
	table.loadSparseIndex()
}

// 加载sstable文件的元数据到内存中
func (table *SSTable) loadMeta() {
	file := table.file
	_, err := file.Seek(0, 0)
	if err != nil {
		log.Println(" error open file ", table.filepath)
		panic(err)
	}
	info, _ := file.Stat()
	//从结尾开始读Meta
	_, err = file.Seek(info.Size()-8*5, 2)
	if err != nil {
		log.Println("Error reading metadata ", table.filepath)
		panic(err)
	}
	_ = binary.Read(file, binary.LittleEndian, &table.tableMeta.version)

	_ = binary.Read(file, binary.LittleEndian, &table.tableMeta.dataStart)

	_ = binary.Read(file, binary.LittleEndian, &table.tableMeta.dataLen)

	_ = binary.Read(file, binary.LittleEndian, &table.tableMeta.indexStart)

	_ = binary.Read(file, binary.LittleEndian, &table.tableMeta.indexLen)
}

// 加载稀疏索引区到内存中
func (table *SSTable) loadSparseIndex() {
	//加载稀疏索引区
	bytes := make([]byte, table.tableMeta.indexLen)
	if _, err := table.file.Seek(table.tableMeta.indexStart, 0); err != nil {
		log.Println(" error seek the file ", table.filepath)
		panic(err)
	}
	if _, err := table.file.Read(bytes); err != nil {
		log.Println(" error read file ", table.filepath)
		panic(err)
	}

	table.sparseIndex = make(map[string]Position)
	//反序列至sstable结构中
	err := json.Unmarshal(bytes, &table.sparseIndex)
	if err != nil {
		log.Println(" error open file ", table.filepath)
		panic(err)
	}
	_, _ = table.file.Seek(0, 0)

	//通过稀疏索引区的数据构造一个有序数组，便于后续快速查找
	keys := make([]string, 0, len(table.sparseIndex))
	for k := range table.sparseIndex {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	table.sortIndex = keys
}
