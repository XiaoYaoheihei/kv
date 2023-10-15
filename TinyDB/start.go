package tinydb

import (
	"log"
	"os"
	"time"
	"tinydb/config"
	"tinydb/memtable"
	"tinydb/sstable"
	"tinydb/wal"
)

// 启动kv数据库
func Start(con config.Config) {
	if database != nil {
		return
	}
	//初始化配置
	log.Println("Loading a Configuration File")
	config.Init(con)
	//初始化数据库
	log.Println("Initializing the database")
	initDatabase(con.DataDir)

	//数据库启动之前进行一次数据压缩
	log.Println("Performing background checks...")
	//检查压缩数据库文件,这里有必要吗？？？？
	database.TableTree.Check()
	//启动后台线程
	go Check()
}

// 初始化数据库,从磁盘文件中还原sstable, wal, memtable等等
func initDatabase(dir string) {
	database = &Database{
		MemoryTree: &memtable.Tree{},
		TableTree:  &sstable.TableTree{},
		Wal:        &wal.Wal{},
	}

	//从磁盘中开始恢复数据
	if _, err := os.Stat(dir); err != nil {
		//刚开始的数据目录不存在
		log.Printf("The %s directory doesn't exist", dir)
		err = os.Mkdir(dir, 0666)
		if err != nil {
			log.Println("Failed to create the database data directory")
			panic(err)
		}
	}
	//从WAL文件中生成BST树
	memTree := database.Wal.Init(dir)
	database.MemoryTree = memTree
	log.Println("Loading databases...")
	database.TableTree.Init(dir)
}

func Check() {
	con := config.GetConfig()
	ticker := time.Tick(time.Duration(con.CheckInterval) * time.Second)
	for _ = range ticker {
		log.Println("Performing background checks...")
		//检查memtable内存数据部分
		checkMem()
		//检查数据库文件sstable是否需要压缩
		database.TableTree.Check()
	}
}

func checkMem() {
	con := config.GetConfig()
	count := database.MemoryTree.Getcount()
	if count < con.PerSize {
		return
	}
	//内存中sstable的数量多于预期值
	log.Println("Compressing memory")
	tmpTree := database.MemoryTree.Swap()
	//将数据存入到sstable中
	database.TableTree.CreateNewTable(tmpTree.GetValue())
	//对当前日志重新设置
	database.Wal.Reset()
}
