# lsm tree 实现一个 kv 存储组件
## 1 存储组件

### 1.1 场景区分

存储组件根据其读写频次可以分为写密集型和读密集型, 面向读密集型典型是 MySQL 的 innodb 存储引擎, 底层基于 B+ tree 实现存储文件的组织和管理; 面向写密集型的典型是基于 lsm tree 实现的各类数据库

### 1.2 lsm 写入策略

在基于kv型存储的组件中, 有原地写和追加写两种:

- 原地写: 要对kv对执行更新操作时, 要 **先找到kv对的位置** , 然后**原地改写**
- 追加写: 将 kv 对以追加的形式**加入到末尾**, 查询时**从后往前**找到第一个符合条件的kv对

原地写的读取效率更加高效, 不像追加写有数据冗余, 有更高的空间利用率; 
追加写读流程退化成了的线性的查询, RocksDB 是在追加写的基础上提高查询的效率.

## 2 lsm tree

### 2.1 追加写存在的问题

- 数据冗余 --> 数据合并, 文件分块
- 读性能低 --> 数据有序存储, 内存 + 磁盘, 预写日志, 内存读写 + 读写分层, 内存数据结构, 磁盘分层, sstable

以下篇幅针对解决这两个问题而设立
### 2.2 数据合并

既然同一组数据会存在冗余, 那么我们可以**启动一个异步的协程将冗余数据进行压缩合并**, 
只保留最新的数据, 既然是异步的协程, 那么我们就要**考虑加锁的粒度**, 防止在性能上有太大的损失

### 2.3 文件分块

异步的压缩写成需要加锁, 会影响写操作的性能, 我们考虑讲数据分块隔离, 以此在物理上隔离压缩操作和写操作,
如果我们能够对数据进行分块, **追加的数据在新块上追加, 异步的压缩协程在旧块上压缩**

### 2.4 数据的有序存储

线性的读取效率是我们不能接受的, 我们可以在kv对的存储方式上下功夫, 在每个分块的内部,
可以实现根据k进行排序, 使用二分的方式进行搜索

### 2.5 内存 + 磁盘

有序存储实际上是与追加写相悖的, 可以在内存中引入一个内存块(memtable)的结构, 在内存中进行过排序之后再放入磁盘, 内存的读写效率要高于磁盘, 可以接受在内存随机IO的代价, 可以在内存执行随机写, 刷新入内存的时候以table为粒度存储, 这样就保证了磁盘文件的天然有序

引入缓存之后, 也引发几个问题:1. 内存是非持久化的, 如何对数据进行持久化 2. 在进行溢写操作的时候, 外部的写操作需要阻塞, 引起性能的损失 3. 内存中是有序存储, 应该如何保存数据, 使用怎样的数据结构

### 2.6 预写日志

如何进行数据持久化, 或者说机器宕机之后如何进行数据恢复, 引入WAL预写日志技术: 数据在内存进行更新的同时, 将操作记录追加写到磁盘的WAL文件中, 这样在宕机之后可以通过查看日志的方式恢复数据

初次之外, WAL也将内存溢写到磁盘过程中丢失的风险消失了, WAL是磁盘顺序IO, 不会成为性能的瓶颈

### 2.7 内存读写 + 读写分层

内存溢写期间写操作如何处理:

处理方式类似于java的垃圾回收(可能), 将memtable赋值一份, 将旧数据归属到只读部分, 专注于执行溢写磁盘流程, 建立一个新的空白部分, 这个空白memtable专注于写操作, 溢写期间读写分离也就不需要阻塞了

### 2.8 内存数据结构

内存的数据需要有序, 保证读写效率, 有两种选择都可以实现读写操作O(logN)时间复杂度: 红黑树 和 调表

读写效率两者差不多, 但是跳表具有更简单的实现和更细的并发粒度两大核心优势

红黑树由于自身的染色机制, 每次写操作都要对整棵树进行加锁, 调表可以针对插入节点局部范围内最大高度加锁, 很多场景可以做到并发写

### 2.9 磁盘分层

磁盘因为分块, 我们能够保证块内的数据有序和去重, 但是块与块之间可能存在冗余kv对, 而且无法做到全局有序

在此基础上引入磁盘分层的概念:

- 磁盘分为level0-levelk总共k+1层
- 每层的块数量一致, 但是容量level(i+1)是level(i)层的T倍(通常取10左右)
- 数据由浅入深, 层层渐进, 当level内数据总量到达阈值, 发起向高一层的归并操作, 这个操作期间进行去重和排序, 保证kv对无重复和全局有序

综上磁盘分层带来的特点:

1. level0 不同块之间存在 kv 对数据冗余且不保证全局有序
2. level1-levelk 单层之内没有冗余kv数据, 且全局有序
3. 不同层之间可能存在kv冗余的数据
4. 最近写入的数据位于浅层, 更早写入的数据位于深层
5. levelk作为最深的一层, 沉淀的数量达到全局90%左右



归并过程, (level1 -> level2)

假设此时level1数据达到阈值, 接下来法刺归并操作:

1. 从level1中随机选择一个块, 可以确定块中k的范围
2. 在level2中确定范围重合的待合并块
3. 与这些块进行归并排序
4. 将新生成的块更具大小规模进行拆分并进行插入
5. 删除level1中老数据
6. 假设这个过程引起了level2的容量超出阈值, 则会引起下一层的合并

### 2.10 sstable

为磁盘的块设计专门的主句结构sstable(sorted string table)

1. 内部会进一步拆分成多个子块
2. 维护一些索引信息, 方便查询
3. lsm tree 维护一个全局索引信息, 记录不同level中每个sstable对应的key范围
4. 每个sstable维护一个布隆过滤器, 用于快速判断一个k是否处于当前sstable中

## 3 lsm tree 读写流程

### 3.1 写流程

就地写模式, 写入内存的active memtable(非可读内存块) ==> active memtable达到阈值之后转换为只读的 readonly memtable并刷新到磁盘成为level0的sstable ==> level(i)容量满之后基于归并的方式合并到下一层

### 3.2 读流程

读active memtable ==> 读 readonly memtable ==> 读 level0(需要按照倒序搜索, 因为level0存在数据冗余) ==> 根据全局索引文件, 以此读level1 ~ levelk(每一次最多读一个sstable) ==> 借助sstable内部布隆过滤器和索引加快查询流程

## 4 布隆过滤器

当我们考虑大量数据进行去重时, 简单方法可以使用一个 `map` 进行

### 4.1 接口定义

```go
// Filter 过滤器接口, 用于辅助 sstable 快速判断一个 key 是否在 block 中
type Filter interface {
    Add(key []byte)                // 向过滤器中添加key
    Exist(bitmap, key []byte) bool // 判断key是否存在于过滤器中
    Hash() []byte                  // 获取过滤器的hash值
    Reset()                        // 重置过滤器
    KeyLen() int                   // 获取key的数量
}
```

