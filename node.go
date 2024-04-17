// LSM tree 树中单个节点的表示

package lsm_tree

import (
	"bytes"
	"os"
	"path"
	"sync"
	"sync/atomic"
)

type Node struct {
	conf          *Config           // 配置信息
	file          string            // sst文件路径
	level         int               // 节点所在的层
	seq           int32             // sst文件序号
	size          uint64            // sst文件大小
	blockToFilter map[uint64][]byte // 数据块到过滤器的映射
	index         []*Index          // 索引
	startKey      []byte            // 节点的起始key
	endKey        []byte            // 节点的结束key
	sstReader     *SSTReader        // sst文件读取器
}

func NewNode(conf *Config, file string, sstReader *SSTReader, level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) *Node {
	return &Node{
		conf:          conf,
		file:          file,
		sstReader:     sstReader,
		level:         level,
		seq:           seq,
		size:          size,
		blockToFilter: blockToFilter,
		index:         index,
		startKey:      index[0].Key,
		endKey:        index[len(index)-1].Key,
	}
}

func NewTree(conf *Config) (*Tree, error) {
	// 1. 构建一个 lsm tree 实例
	t := Tree{
		conf:          conf,
		memCompactC:   make(chan *memTableCompactItem),
		levelCompactC: make(chan int),
		stopc:         make(chan struct{}),
		levelToSeq:    make([]atomic.Int32, conf.MaxLevel),
		nodes:         make([][]*Node, conf.MaxLevel),
		levelLocks:    make([]sync.RWMutex, conf.MaxLevel),
	}

	// 2. 读取 sst 文件还原出 lsm tree
	if err := t.constructTree(); err != nil {
		return nil, err
	}

	// 3. 运行lsm tree 压缩调整协程
	go t.compact()

	// 4. 读取 wal 还原出 memtable
	if err := t.constructMemTable(); err != nil {
		return nil, err
	}

	// 5. 返回 lsm tree 实例
	return &t, nil
}
func (n *Node) GetAll() ([]*KV, error) {
	return n.sstReader.ReadData()
}

// Get 查看是否在节点中
func (n *Node) Get(key []byte) ([]byte, bool, error) {
	// 通过索引定位到具体的块
	index, ok := n.binarySearchIndex(key, 0, len(n.index)-1)
	if !ok {
		return nil, false, nil
	}

	// 布隆过滤器辅助判断 key 是否存在
	bitmap := n.blockToFilter[index.PrevBlockOffset]
	if ok = n.conf.Filter.Exist(bitmap, key); !ok {
		return nil, false, nil
	}

	// 读取对应的块
	block, err := n.sstReader.ReadBlock(index.PrevBlockOffset, index.PrevBlockSize)
	if err != nil {
		return nil, false, err
	}

	// 将块数据转为对应的 kv 对
	kvs, err := n.sstReader.ReadBlockData(block)
	if err != nil {
		return nil, false, err
	}

	for _, kv := range kvs {
		if bytes.Equal(kv.Key, key) {
			return kv.Value, true, nil
		}
	}

	return nil, false, nil
}

// 二分查找，key 可能从属的 block index
func (n *Node) binarySearchIndex(key []byte, start, end int) (*Index, bool) {
	if start == end {
		return n.index[start], bytes.Compare(n.index[start].Key, key) >= 0
	}

	// 目标块，保证 key <= index[i].key && key > index[i-1].key
	mid := start + (end-start)>>1
	if bytes.Compare(n.index[mid].Key, key) < 0 {
		return n.binarySearchIndex(key, mid+1, end)
	}

	return n.binarySearchIndex(key, start, mid)
}
func (n *Node) Size() uint64 {
	return n.size
}

func (n *Node) Start() []byte {
	return n.startKey
}

func (n *Node) End() []byte {
	return n.endKey
}

func (n *Node) Index() (level int, seq int32) {
	level, seq = n.level, n.seq
	return
}

func (n *Node) Destroy() {
	n.sstReader.Close()
	_ = os.Remove(path.Join(n.conf.Dir, n.file))
}

func (n *Node) Close() {
	n.sstReader.Close()
}
