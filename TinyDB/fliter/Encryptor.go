package fliter

import (
	"math"

	"github.com/spaolacci/murmur3"
)

// 通过murmur3实现的hash编码模块
type Encryptor struct {
}

func NewEncryptor() *Encryptor {
	return &Encryptor{}
}

// 每一次加密获取哈希的操作
func (e *Encryptor) Encrypt(origin string, seed int) int32 {
	hasher := murmur3.New32WithSeed(uint32(seed))
	//将origin输入并且获取相应的哈希值
	_, _ = hasher.Write([]byte(origin))
	//将哈希值转化为int32位
	return int32(hasher.Sum32() % math.MaxInt32)
}
