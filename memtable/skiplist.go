package memtable

import (
	"bytes"
	"math/rand"
	"time"
)

type skipNode struct {
	nexts      []*skipNode // 实现跳表的多层索引
	key, value []byte      // key - value
}

// Skiplist 跳表 不保证并发安全
type Skiplist struct {
	head       *skipNode // 跳表头节点
	entriesCnt int       // 跳表节点数量
	size       int       // 跳表大小, kv对的数据量大小
}

// NewSkiplist 构建跳表实例
func NewSkiplist() MemTable {
	return &Skiplist{
		head: &skipNode{},
	}
}

// Get 从跳表中获取数据
func (s *Skiplist) Get(key []byte) ([]byte, bool) {
	if node := s.getNode(key); node != nil {
		return node.value, true
	}
	return nil, false
}

// All 获取跳表中所有数据
func (s *Skiplist) All() []*KV {
	if len(s.head.nexts) == 0 {
		return nil
	}

	kvs := make([]*KV, 0, s.entriesCnt)
	// 从最低层开始遍历
	for move := s.head; move.nexts[0] != nil; move = move.nexts[0] {
		kvs = append(kvs, &KV{
			Key:   move.nexts[0].key,
			Value: move.nexts[0].value,
		})
	}
	return kvs
}

// Put 向跳表中添加数据, 如果key已经存在, 则更新value
func (s *Skiplist) Put(key, value []byte) {
	// 如果可以从跳表中找到key, 则更新value
	if node := s.getNode(key); node != nil {
		// 更新跳表大小, 因为要落盘到文件中, 所以需要记录数据量大小
		s.size += len(value) - len(node.value)
		// 更新value
		node.value = value
		return
	}

	// 如果key不存在就要插入节点
	s.size += len(key) + len(value)
	s.entriesCnt++
	// 随机生成一个高度
	newNodeHeight := s.roll()
	// 如果跳表的高度不足, 则补充
	if len(s.head.nexts) < newNodeHeight {
		// 补充差值
		dif := make([]*skipNode, newNodeHeight-len(s.head.nexts))
		// 从最后一个节点开始补充
		s.head.nexts = append(s.head.nexts, dif...)
	}

	// 构建新的节点
	newNode := skipNode{
		nexts: make([]*skipNode, newNodeHeight),
		key:   key,
		value: value,
	}

	// 每层按照key的大小插入
	move := s.head
	for level := newNodeHeight - 1; level >= 0; level-- {
		// 向右移动, 直到下一个节点为空或者下一个节点的key大于等于给定的key, 说明当前节点是最接近给定key的节点
		for move.nexts[level] != nil && bytes.Compare(move.nexts[level].key, key) < 0 {
			move = move.nexts[level]
		}
		// 插入节点
		newNode.nexts[level] = move.nexts[level]
		move.nexts[level] = &newNode
	}
}

func (s *Skiplist) Len() int {
	return s.size
}

func (s *Skiplist) EntriesCnt() int {
	return s.entriesCnt
}

func (s *Skiplist) getNode(key []byte) *skipNode {
	move := s.head
	// 从最高层开始查找
	for level := len(s.head.nexts) - 1; level >= 0; level-- {
		// 向右移动, 直到下一个节点为空或者下一个节点的key大于等于给定的key, 说明当前节点是最接近给定key的节点
		for move.nexts[level] != nil && bytes.Compare(move.nexts[level].key, key) < 0 {
			move = move.nexts[level]
		}
		// 如果下一个节点的key等于给定的key, 说明找到了, 否则继续向下一层查找
		if move.nexts[level] != nil && bytes.Compare(move.nexts[level].key, key) == 0 {
			return move.nexts[level]
		}
	}
	return nil
}

func (s *Skiplist) roll() int {
	var level int
	rander := rand.New(rand.NewSource(time.Now().UnixNano()))
	for rander.Intn(2) == 1 {
		level++
	}
	return level + 1
}

// Size 跳表数据量大小，单位 byte
func (s *Skiplist) Size() int {
	return s.size
}
