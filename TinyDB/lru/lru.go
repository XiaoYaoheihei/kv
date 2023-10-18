package lru

import (
	"container/list"
	"sync"
)

type Cache struct {
	maxnumber int
	//双向链表存储LRU元素
	ll *list.List
	//value是指向双向链表的指针
	cache map[string]*list.Element
	//key失效时的回调
	Callback func(key string, value interface{})
	//使用锁保证并发安全
	mu sync.Mutex
}

type entry struct {
	key   string
	value interface{}
}

func New(max int, cb func(key string, value interface{})) *Cache {
	return &Cache{
		maxnumber: max,
		ll:        list.New(),
		cache:     make(map[string]*list.Element),
		Callback:  cb,
	}
}

func (c *Cache) Get(key string) (value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if ele, ok := c.cache[key]; ok {
		//移动到队头
		c.ll.MoveToFront(ele)
		kv := ele.Value.(*entry)
		return kv.value
	}
	//没有找到返回nil值进行相应的判断
	return nil
}

func (c *Cache) Set(key string, value interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.Len() >= c.maxnumber {
		c.Delete()
	}
	//更改元素值
	if ele, ok := c.cache[key]; ok {
		c.ll.MoveToFront(ele)
		//类型转化
		kv := ele.Value.(*entry)
		kv.value = value
	} else {
		//增加元素值，添加到队头
		ele = c.ll.PushFront(&entry{key: key, value: value})
		c.cache[key] = ele
	}

}

func (c *Cache) Delete() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cache == nil {
		return
	}
	//拿到队尾的元素
	ele := c.ll.Back()
	if ele != nil {
		c.ll.Remove(ele)
		kv := ele.Value.(*entry)
		//删除map中的元素，会释放相应的内存吗
		delete(c.cache, kv.key)
		//调用相应的回调函数
		if c.Callback != nil {
			c.Callback(kv.key, kv.value)
		}
	} else {
		return
	}
}

func (c *Cache) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}
