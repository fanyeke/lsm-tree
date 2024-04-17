package memtable

// MemTableConstructor MemTable 构造器
type MemTableConstructor func() MemTable

// MemTable MemTable 内存表接口
type MemTable interface {
	Put(key, value []byte)         // 写入数据
	Get(key []byte) ([]byte, bool) // 读取数据, flag 为 true 时表示数据存在
	All() []*KV                    // 获取所有数据
	Size() int                     // 获取数据长度
	EntriesCnt() int               // 获取数据数量
}
type KV struct {
	Key, Value []byte
}
