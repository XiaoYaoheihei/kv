package fliter

import (
	"github.com/spaolacci/murmur3"
)

// 通过murmur3实现的hash编码模块
type Encryptor struct {
}

func NewEncryptor() *Encryptor {
	return &Encryptor{}
}

// 每一次加密获取哈希的操作
func (e *Encryptor) Encrypt(origin string, seed int) uint64 {
	hasher := murmur3.New64WithSeed(uint32(seed))
	//将origin输入并且获取相应的哈希值
	_, _ = hasher.Write([]byte(origin))
	//将哈希值转化为int64位
	return hasher.Sum64()
}
