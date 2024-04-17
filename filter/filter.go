package filter

// Filter 过滤器接口, 用于辅助 sstable 快速判断一个 key 是否在 block 中
type Filter interface {
	Add(key []byte)                // 向过滤器中添加key
	Exist(bitmap, key []byte) bool // 判断key是否存在于过滤器中
	Hash() []byte                  // 获取过滤器的hash值
	Reset()                        // 重置过滤器
	KeyLen() int                   // 获取key的数量
}
