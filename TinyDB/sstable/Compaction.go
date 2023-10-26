package sstable

import (
	"fmt"
	"log"
	"os"
	"time"
	"tinydb/config"
	"tinydb/kv"
	"tinydb/memtable"
)

//开始合并相应level的sstable文件

// 检查是否需要压缩数据库文件
func (t *TableTree) Check() {
	t.compaction()
}

// 开始压缩文件
func (t *TableTree) compaction() {
	con := config.GetConfig()

	for levelIndex, _ := range t.levels {
		//获取当前层数中的总字节大小,转化为MB
		allTableSize := int(t.GetLevelsize(levelIndex) / 1024 / 1024)
		//如果sstable文件的数量和容量任何一个超过阈值大小
		//合并本层的sstable文件
		if t.getCount(levelIndex) > con.PerSize || allTableSize > levelSize[levelIndex] {
			t.compactionToNextLevel(levelIndex)
		}
	}
	fmt.Println("This peroid had completed compaction")
}

// 压缩当前层的文件到下一层
func (t *TableTree) compactionToNextLevel(level int) {
	log.Println("Compressing layer ", level, " files")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("Completed compression,consumption of time : ", elapse)
	}()

	log.Printf("Compressing layer %d.db files\r\n", level)

	//将当前层数的所有sstable合并到一个有序二叉树中
	memTree := &memtable.Tree{}
	memTree.Init()

	currentNode := t.levels[level]
	//数据缓冲
	dataCache := make([]byte, levelSize[level])

	t.lock.Lock()
	for currentNode != nil {
		currentTable := currentNode.table
		dataBlock := dataCache[0:currentTable.tableMeta.dataLen]

		//将文件指针偏移到文件开头处,并且开始读取数据区的所有数据
		if _, err := currentTable.file.Seek(0, 0); err != nil {
			log.Println(" error open file ", currentTable.filepath)
			panic(err)
		}
		//这里明显还可以优化，这里设计的每一次读取都是从disk中读取
		//应该先从cache中读取，然后才从磁盘中读取
		if _, err := currentTable.file.Read(dataBlock); err != nil {
			log.Println(" error read file ", currentTable.filepath)
			panic(err)
		}

		//现在默认索引区的数据和数据区的数据是一致的
		//根据索引区信息开始读取每一个元素
		for k, pos := range currentTable.sparseIndex {
			if pos.Deleted {
				//该元素是待删除的,插入到二叉树中
				//此时这里的节点如果不存在的话会返回nil值，后面需要看看
				memTree.Delete(k)
			} else {
				value, err := kv.Decode(dataBlock[pos.Start:(pos.Start + pos.Len)])
				if err != nil {
					log.Fatal(err)
				}
				memTree.Set(k, value.Value)
			}
		}
		currentNode = currentNode.next
	}
	t.lock.Unlock()

	//将memTree中的数据压缩并且合并成一个sstable文件
	allValues := memTree.GetValue()
	newLevel := level + 1
	//可以设置一个最大支持层数
	//后续可以设计定期删除相应的数据
	if newLevel == 10 {
		//如果合并的文件是最后一层的，直接将这一层原来的所有数据全部删除
		//新构造这一层相应的文件
		oldNode := t.levels[9]
		t.clearLevel(oldNode)
		t.creatTable(allValues, 9)
		return
	}
	//开始创建新的sstable,并插入到相应的层
	t.creatTable(allValues, newLevel)
	//清理该level的所有文件
	oldNode := t.levels[level]
	t.levels[level] = nil
	t.clearLevel(oldNode)
}

// 清除压缩完之后的当前层
func (t *TableTree) clearLevel(oldNode *tableNode) {
	t.lock.Lock()
	defer t.lock.Unlock()

	for oldNode != nil {
		//关闭文件描述符
		err := oldNode.table.file.Close()
		if err != nil {
			log.Println(" error close file,", oldNode.table.filepath)
			panic(err)
		}
		//删除table对应的物理文件，释放磁盘空间
		err = os.Remove(oldNode.table.filepath)
		if err != nil {
			log.Println(" error delete file,", oldNode.table.filepath)
			panic(err)
		}
		//将对象设置为nil，垃圾回收会自动回收这一部分内存
		//但是像File这个文件描述符，并没有主动关闭，也就是还存在引用关系
		//设置为nil的话OS并不会主动关闭文件描述符更不会释放此描述符对应的内存
		//将对象引用置为 nil 只是失去了对该对象的引用
		oldNode.table.file = nil
		oldNode.table = nil
		oldNode = oldNode.next
	}
}
