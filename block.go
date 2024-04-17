package lsm_tree

import (
	"bytes"
	"encoding/binary"
	"github.com/fanyeke/lsm-tree/util"
	"io"
)

type Block struct {
	conf       *Config       // 配置信息
	buffer     [30]byte      // 30 字节的缓冲区, 进行数据转移时的缓冲区
	record     *bytes.Buffer // 记录全量数据的缓冲区
	entriesCnt int           // kv对的数量
	prevKey    []byte        // 最晚一笔写入的key
}

func NewBlock(conf *Config) *Block {
	return &Block{
		conf:   conf,
		record: bytes.NewBuffer([]byte{}),
	}
}

// Append 向数据块中添加数据, 追加一组key-value
func (b *Block) Append(key, value []byte) {
	// 兜底执行: 将 prevKey 为当前 key, 累加 entriesCnt
	defer func() {
		b.prevKey = append(b.prevKey[:0], key...)
		b.entriesCnt++
	}()

	// 获取与之前的key共享的前缀长度
	sharePrefixLen := util.SharedPrefixLen(b.prevKey, key)

	// 分别设置共享key的长度 || 剩余key长度 || value长度
	n := binary.PutUvarint(b.buffer[:], uint64(sharePrefixLen))
	n += binary.PutUvarint(b.buffer[n:], uint64(len(key)-sharePrefixLen))
	n += binary.PutUvarint(b.buffer[n:], uint64(len(value)))

	// 将共享key长度 || 剩余key长度 || value长度 写入到记录缓冲区
	_, _ = b.record.Write(b.buffer[:n])

	// 将剩余key || value 写入到记录缓冲区
	b.record.Write(key[sharePrefixLen:])
	b.record.Write(value)

}

// FlushTo 把块中的数据溢写到 dest writer 中
func (b *Block) FlushTo(dest io.Writer) (uint64, error) {
	defer b.clear()
	n, err := dest.Write(b.ToBytes())
	return uint64(n), err
}

// 清理数据块中的数据
func (b *Block) clear() {
	b.entriesCnt = 0
	b.prevKey = b.prevKey[:0]
	b.record.Reset()
}

// ToBytes 将数据块中的数据转为 byte 数组
func (b *Block) ToBytes() []byte {
	return b.record.Bytes()
}

// Size 获取数据块的大小，单位 byte
func (b *Block) Size() int {
	return b.record.Len()
}
