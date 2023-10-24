package tinydb

import (
	"log"
	"os"
	"path/filepath"
	"time"
	"tinydb/config"
	"tinydb/sstable"
	"tinydb/wal"
)

// 启动kv数据库
func Start(con config.Config) {
	if database != nil {
		return
	}
	//初始化配置
	log.SetFlags(log.Lshortfile | log.Lmicroseconds | log.Ldate)
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

// 初始化数据库,从磁盘中根据wal文件还原memtable
// 并根据当前的sstable构建tableTree
func initDatabase(dir string) {
	database = &Database{
		MemoryTree:   nil,
		ImmutableMem: nil,
		TableTree:    &sstable.TableTree{},
		Wal:          nil,
		Wal1:         &wal.Wal{},
		Wal2:         &wal.Wal{},
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
	database.Wal = database.Wal1
	//从WAL文件中生成BST树
	database.MemoryTree = database.Wal1.Init(dir, 1)
	//初始化辅助日志
	database.Wal2.Init(dir, 2)
	log.Println("All log has been created")
	log.Println("Loading databases...")
	database.TableTree.Init(dir)
}

// 定期检查memtable中的数据是否超出阈值
// 如果超出阈值，将memtable转化为immutable
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
	if count < con.Threshold {
		return
	}
	//内存中memtable的节点数量多于预期值
	log.Println("Compressing memory")
	database.ImmutableMem = database.MemoryTree.Swap()
	//每次都交换wal文件指针
	if filepath.Base(database.Wal.Pathname) == "wal1.log" {
		database.Wal = database.Wal2
		database.Wal1.Reset()
	} else {
		database.Wal = database.Wal1
		database.Wal2.Reset()
	}
	log.Println("Resetting the wal.log file success")
	//将immutableMem中的数据存入到sstable中
	database.TableTree.CreateNewTable(database.ImmutableMem.GetValue())
}
