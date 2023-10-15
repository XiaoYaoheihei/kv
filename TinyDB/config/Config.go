package config

import "sync"

// k-v数据库启动配置
type Config struct {
	//数据目录
	DataDir string
	//0层所有sstable文件大小总和的最大值，为Mb
	//超过此阈值，该层sstable会被压缩到下一层
	Level0Size int
	//每层中sstable数量的阈值
	PerSize int
	//memtable中kv的最大值，超出阈值会被保存到sstable中
	Threshold int
	//做一次检查工作的时间间隔
	CheckInterval int
}

// 常驻内存
var config Config

// 用于确保初始化Init函数只会执行一次
var once *sync.Once = &sync.Once{}

// 初始化数据库配置
func Init(con Config) {
	once.Do(func() {
		config = con
	})
}

// 获取数据库配置
func GetConfig() Config {
	return config
}
