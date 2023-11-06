## 一个简单的lsm-tree持久kv对存储引擎

------------------------------------------

本项目是在了解了lsm-tree的原理以及学习了LevelDB的设计架构后自主实现的一个简单的lsm-tree持久化键值对存储引擎。

### 实现细节：

- 并发内存表memtable使用了mutex+skiplist的方式实现
- memtable有一个内存大小阈值（可自己配置），当达到这个阈值之后，memtable会变为ImmutableMemtable，并且会新建一个memtable，这个ImmutableMemtable中的数据最后会持久化到sstable文件中
- 预写日志WAL是通过向文件中追加{len，k-v}的简单方式实现
- 当memtable变为immutableMemtable的时候，为了保证日志文件的正确性，采用了双缓冲日志的方式
- 压实采取Tiering策略来减少写放大
- 每一个sstable对象都保存着一个keys列表和每一个key对应的哈希索引，方便查找

### 待改进的地方：

- 内存后续可以考虑LevelDB的无锁SkipList
- 为了更加高效查找sstable对象中管理的文件数据，可以采取BloomFliter+稀疏索引的方式（正在进行中√）
- 读取文件的数据的时候，考虑对block数据块和sstable对象分别进行缓存来提高性能，可以采用LRU的方式
- 后续会尝试采用 L-Leveling 等其他压实策略

