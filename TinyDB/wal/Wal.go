package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
	"path"
	"sync"
	"tinydb/kv"
	"tinydb/memtable"
)

type Wal struct {
	file     *os.File
	pathname string
	lock     *sync.Mutex
}

// 日志的初始化
func (w *Wal) Init(dir string) *memtable.Tree {
	log.Println("loading wal.log...")
	walpath := path.Join(dir, "wal.log")
	f, err := os.OpenFile(walpath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("the wal.log cannot be create")
		panic(err)
	}
	log.Println("wal.log had been create")
	w.file = f
	w.pathname = walpath
	w.lock = &sync.Mutex{}
	return w.LoadtoMem()
}

// 记录日志
func (w *Wal) Writer(value kv.Value) {
	//首先转化成json格式的字符串，然后再转化成二进制格式的数据进行存储
	data, _ := json.Marshal(value)
	log.Println(data)
	//首先以小端的方式写入8字节的数据长度
	err := binary.Write(w.file, binary.LittleEndian, int64(len(data)))
	if err != nil {
		log.Println(err)
	}
	//然后以小端的方式写入数据
	err = binary.Write(w.file, binary.LittleEndian, data)
	if err != nil {
		log.Println(err)
	}
}

// 加载WAL文件中的数据到内存表memtable中
func (w *Wal) LoadtoMem() *memtable.Tree {
	w.lock.Lock()
	defer w.lock.Unlock()

	info, _ := os.Stat(w.pathname)
	size := info.Size()
	tree := &memtable.Tree{}
	tree.Init()
	if size == 0 {
		//空的wal文件
		return tree
	}

	_, err := w.file.Seek(0, 0)
	if err != nil {
		log.Println("failed to seek the wal.log")
		panic(err)
	}
	//将文件指针移动到最后，方便之后操作
	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Println("Failed to seek the wal.log")
			panic(err)
		}
	}(w.file, size-1, 0)

	//首先将文件内容全部读取到字节切片中
	data := make([]byte, size)
	//读取len（data）字节写入data
	_, err = w.file.Read(data)
	if err != nil {
		log.Println("failed to read the wal.log")
		panic(err)
	}

	//开始根据文件中的具体元素构造整颗树
	//每一个元素的字节数量
	datelen := int64(0)
	//当前索引
	index := int64(0)
	for index < size {
		//首先读取前8个字节,读取该元素的长度
		indexData := data[index:(index + 8)]
		//从一个切片构造一个Buffer
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &datelen)
		if err != nil {
			log.Println("Failed to open the wal.log")
			panic(err)
		}

		index += 8
		//获取具体的数据
		dataContent := data[index:(index + datelen)]
		var value kv.Value
		//将二进制内容反序列化成kv结构
		err = json.Unmarshal(dataContent, &value)
		if err != nil {
			log.Println("Failed to open the wal.log")
			panic(err)
		}

		if value.Delete {
			tree.Delete(value.Key)
		} else {
			tree.Set(value.Key, value.Value)
		}
		index += datelen
	}
	return tree
}

func (w *Wal) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()

	log.Println("Resetting the wal.log file")
	err := w.file.Close()
	if err != nil {
		log.Println("Close the wal file fd fail")
		panic(err)
	}
	w.file = nil
	err = os.Remove(w.pathname)
	if err != nil {
		log.Println("Delete wal file from disk fail")
		panic(err)
	}
	f, err := os.OpenFile(w.pathname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("Failed to creat new wal file")
		panic(err)
	}
	w.file = f
}
