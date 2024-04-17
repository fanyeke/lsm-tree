package lsm_tree

import (
	"bytes"
	"encoding/binary"
	"github.com/fanyeke/lsm-tree/util"
	"os"
	"path"
)

type SSTWriter struct {
	conf          *Config           // 配置信息
	dest          *os.File          // sst文件实际对应的磁盘文件
	dataBuf       *bytes.Buffer     // 数据块缓冲区
	filterBuf     *bytes.Buffer     // 过滤器块缓冲区
	indexBuf      *bytes.Buffer     // 索引块缓冲区
	blockToFilter map[uint64][]byte // 数据块到过滤器的映射
	index         []*Index          // 索引

	dataBlock     *Block   // 数据块
	filterBlock   *Block   // 过滤器块
	indexBlock    *Block   // 索引块
	assistScratch [20]byte // 辅助缓冲区

	prevKey         []byte // 上一笔写入的key
	prevBlockOffset uint64 // 上一笔写入的数据块的偏移量
	prevBlockSize   uint64 // 上一笔写入的数据块的大小
}

// Index 用于在sstable中快速检索block的索引
type Index struct {
	Key             []byte // block的起始key
	PrevBlockOffset uint64 // 上一个block的偏移量
	PrevBlockSize   uint64 // 上一个block的大小
}

func NewSSTWriter(file string, conf *Config) (*SSTWriter, error) {
	dest, err := os.OpenFile(path.Join(conf.Dir, file), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &SSTWriter{
		conf:          conf,
		dest:          dest,
		dataBuf:       bytes.NewBuffer([]byte{}),
		filterBuf:     bytes.NewBuffer([]byte{}),
		indexBuf:      bytes.NewBuffer([]byte{}),
		blockToFilter: make(map[uint64][]byte),
		dataBlock:     NewBlock(conf),
		filterBlock:   NewBlock(conf),
		indexBlock:    NewBlock(conf),
		prevKey:       []byte{},
	}, err
}

type KV struct {
	Key, Value []byte
}

func (s *SSTWriter) Append(key, value []byte) {
	// 如果开启了新的数据快需要添加索引
	if s.dataBlock.entriesCnt == 0 {
		s.insertIndex(key)
	}

	// 写入数据块
	s.dataBlock.Append(key, value)
	// 写入过滤器块
	s.conf.Filter.Add(key)
	// 记录下一个key
	s.prevKey = key

	//如果数据大小超过了阈值, 则写将其加入dataBuf, 并重置块
	if s.dataBlock.Size() >= s.conf.SSTDataBlockSize {
		s.refreshBlock()
	}
}

func (s *SSTWriter) insertIndex(key []byte) {
	// 获取索引的 key
	indexKey := util.GetSeparatorBetween(s.prevKey, key)
	n := binary.PutUvarint(s.assistScratch[0:], s.prevBlockOffset)
	n += binary.PutUvarint(s.assistScratch[n:], s.prevBlockSize)

	s.indexBlock.Append(indexKey, s.assistScratch[:n])
	s.index = append(s.index, &Index{
		// 索引 key
		Key: indexKey,
		// 前一个 block 的 offset
		PrevBlockOffset: s.prevBlockOffset,
		// 前一个 block 的 size
		PrevBlockSize: s.prevBlockSize,
	})
}

func (s *SSTWriter) refreshBlock() {
	if s.conf.Filter.KeyLen() == 0 {
		return
	}

	s.prevBlockOffset = uint64(s.dataBuf.Len())
	// 获取 block 对应的 filter bitmap
	filterBitmap := s.conf.Filter.Hash()
	s.blockToFilter[s.prevBlockOffset] = filterBitmap
	n := binary.PutUvarint(s.assistScratch[0:], s.prevBlockOffset)
	// 将布隆过滤器数据写入到 filter bitmap
	s.filterBlock.Append(s.assistScratch[:n], filterBitmap)
	// 重置布隆过滤器
	s.conf.Filter.Reset()

	// 将 block 的数据添加到缓冲区
	s.prevBlockSize, _ = s.dataBlock.FlushTo(s.dataBuf)
}

// Finish 完成 sstable 的全部处理流程，包括将其中的数据溢写到磁盘，并返回信息供上层的 lsm 获取缓存
func (s *SSTWriter) Finish() (size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	// 完成最后一个块的处理
	s.refreshBlock()
	// 补齐最后一个 index
	s.insertIndex(s.prevKey)

	// 将布隆过滤器块写入缓冲区
	_, _ = s.filterBlock.FlushTo(s.filterBuf)
	// 将索引块写入缓冲区
	_, _ = s.indexBlock.FlushTo(s.indexBuf)

	// 处理 footer，记录布隆过滤器块起始、大小、索引块起始、大小
	footer := make([]byte, s.conf.SSTFooterSize)
	size = uint64(s.dataBuf.Len())
	n := binary.PutUvarint(footer[0:], size)
	filterBufLen := uint64(s.filterBuf.Len())
	n += binary.PutUvarint(footer[n:], filterBufLen)
	size += filterBufLen
	n += binary.PutUvarint(footer[n:], size)
	indexBufLen := uint64(s.indexBuf.Len())
	n += binary.PutUvarint(footer[n:], indexBufLen)
	size += indexBufLen

	// 依次写入文件
	_, _ = s.dest.Write(s.dataBuf.Bytes())
	_, _ = s.dest.Write(s.filterBuf.Bytes())
	_, _ = s.dest.Write(s.indexBuf.Bytes())
	_, _ = s.dest.Write(footer)

	blockToFilter = s.blockToFilter
	index = s.index
	return
}
func (s *SSTWriter) Size() uint64 {
	return uint64(s.dataBuf.Len())
}

func (s *SSTWriter) Close() {
	_ = s.dest.Close()
	s.dataBuf.Reset()
	s.indexBuf.Reset()
	s.filterBuf.Reset()
}
