package sstable

import (
	"encoding/binary"
	"log"
	"os"
)

//管理sstable文件对应的接口

// 获取.db文件的大小
func (table *SSTable) GetDbsize() int64 {
	info, err := os.Stat(table.filepath)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}

// 将数据写入到文件当中
func writeDataToFile(filepath string, dataArea []byte, indexArea []byte, meta Meta) {
	//此时以只写的方式打开相应文件
	file, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE, 0666)
	defer file.Close()

	if err != nil {
		log.Fatal(" error create file,", err)
	}
	//先写所有的数据
	_, err = file.Write(dataArea)
	if err != nil {
		log.Fatal(" error write file,", err)
	}
	//写稀疏索引区的数据
	_, err = file.Write(indexArea)
	if err != nil {
		log.Fatal(" error write file,", err)
	}
	//写入元数据到数据末尾
	err = binary.Write(file, binary.LittleEndian, &meta.version)
	if err != nil {
		log.Fatal(err)
	}
	err = binary.Write(file, binary.LittleEndian, &meta.dataStart)
	if err != nil {
		log.Fatal(err)
	}
	err = binary.Write(file, binary.LittleEndian, &meta.dataLen)
	if err != nil {
		log.Fatal(err)
	}
	err = binary.Write(file, binary.LittleEndian, &meta.indexStart)
	if err != nil {
		log.Fatal(err)
	}
	err = binary.Write(file, binary.LittleEndian, &meta.indexLen)
	if err != nil {
		log.Fatal(err)
	}
	//上述所有的write都只是把数据暂时写入到文件缓冲区中，并没有立即刷盘
	//sync函数将文件缓冲区中的数据强制刷新/写入到磁盘中
	err = file.Sync()
	if err != nil {
		log.Fatal(" error write file from file's buffer,", err)
	}
}
