package tinydb

import (
	"encoding/json"
	"log"
	"tinydb/kv"
	"tinydb/memtable"
	"tinydb/sstable"
	"tinydb/wal"
)

type Database struct {
	//当前内存中可读可写的内存表
	MemoryTree *memtable.Tree
	//不可继续写的内存表,只能读
	ImmutableMem *memtable.Tree
	//sstable
	TableTree *sstable.TableTree
	//日志文件句柄
	Wal *wal.Wal
	//两个日志文件，保证平稳过渡
	Wal1 *wal.Wal
	Wal2 *wal.Wal
}

// 全局唯一的数据库
var database *Database

// 整个数据库对外提供的接口
// get获取元素
func Get[T any](key string) (T, bool) {
	log.Print("Get: ", key)
	//首先在可读可写的内存表中查询memtable中有无数据
	value, res := database.MemoryTree.Search(key)
	if res == kv.Success {
		return getInstance[T](value.Kv.Value)
	}
	//从immutableMem中寻找对应数据
	value, res = database.ImmutableMem.Search(key)
	if res == kv.Success {
		return getInstance[T](value.Kv.Value)
	}
	//两个内存表中都没有找到相应的数据
	//开始从其余的sstable文件中查找
	log.Print("Get from sstable file")

	if database.TableTree != nil {
		value, res := database.TableTree.SearchTree(key)
		if res == kv.Success {
			return getInstance[T](value.Value)
		} else if res == kv.Deleted {
			//如果数据已经被删除
			var nil T
			return nil, false
		}
	}
	//数据不存在
	var nil T
	return nil, false
}

// 将字节数组转化为类型对象
func getInstance[T any](data []byte) (T, bool) {
	var value T
	err := json.Unmarshal(data, &value)
	if err != nil {
		log.Print(err)
	}
	return value, true
}

// set插入任意元素
func Set[T any](key string, value T) bool {
	log.Print("Set: ", key)
	//首先将数据转化为二进制序列
	data, err := convert[T](value)
	if err != nil {
		log.Println(err)
		return false
	}
	//在内存表中写入相关值
	_, _ = database.MemoryTree.Set(key, data)

	//写入wal日志
	database.Wal.Writer(kv.Value{
		Key:    key,
		Value:  data,
		Delete: false,
	})
	return true
}

// 将任意元素值转化为二进制
func convert[T any](value T) ([]byte, error) {
	return json.Marshal(value)
}

// delete删除元素
func Delete[T any](key string) bool {
	log.Print("Delete: ", key)
	_, res := database.MemoryTree.Delete(key)
	//数据不存在的情况下直接返回
	if res == false {
		log.Print("this key not exsit")
		return false
	}
	//数据存在的情况下,写入日志处理
	database.Wal.Writer(kv.Value{
		Key:    key,
		Value:  nil,
		Delete: true,
	})
	return true
}

// 删除元素并且获得旧值,bool表示有无旧值
func DeleteAndGet[T any](key string) (T, bool) {
	log.Print("Delete: ", key)
	value, res := database.MemoryTree.Delete(key)
	if res {
		//找到了旧值将操作写入日志处理
		database.Wal.Writer(kv.Value{
			Key:    key,
			Value:  nil,
			Delete: true,
		})
		return getInstance[T](value.Value)
	}
	//不存在旧值
	var nil T
	return nil, false
}
