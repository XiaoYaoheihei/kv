package fliter

import (
	"strconv"
)

type BloomFliter struct {
	//m是bitmap长度
	//n是已经设置的元素个数
	//k是设置的哈希函数
	m, n, k int32
	//每一个元素都是32个bit位
	bitmap []int
	//哈希函数
	encryptor *Encryptor
}

func NewBloomFliter(m, k int32, hashFunc *Encryptor) *BloomFliter {
	return &BloomFliter{
		m:         m,
		k:         k,
		bitmap:    make([]int, m/32+1),
		encryptor: hashFunc,
	}
}

// 添加元素
func (b *BloomFliter) Set(val string) {
	b.n++
	for _, offset := range b.getKEncrypted(val) {
		index := offset >> 5
		bitOffset := offset & 31
		//通过 | 操作将对应的bit置1
		b.bitmap[index] |= (1 << bitOffset)
	}
}

// 判读一个元素是否存在
func (b *BloomFliter) Exist(val string) bool {
	//首先获取k个bit位的偏移量
	for _, offset := range b.getKEncrypted(val) {
		index := offset >> 5     // 等价于 / 32
		bitOffset := offset & 31 // 等价于 % 32
		//如果有任何一个bit位标识为0，证明不存在
		if b.bitmap[index]&(1<<bitOffset) == 0 {
			return false
		}
	}
	return true
}

// 获取一个元素val对应的k个bit位的偏移量offset
func (b *BloomFliter) getKEncrypted(val string) []int32 {
	//所有的bit位
	encrypteds := make([]int32, 0, b.k)
	origin := val
	for i := 0; int32(i) < b.k; i++ {
		encrypted := b.encryptor.Encrypt(origin)
		//获取hash值之后还需要根据长度进行取模
		encrypted = encrypted % b.m
		encrypteds = append(encrypteds, encrypted%b.m)
		if int32(i) == b.k-1 {
			break
		}
		//首次映射时以元素val作为哈希函数的输入获取哈希值
		//接下来的每一次都通过添加后缀的方式作为输入获取新的hash值
		origin = origin + strconv.Itoa(int(encrypted))
	}
	return encrypteds
}
