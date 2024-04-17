// 整棵lsm tree的表示

package lsm_tree

import (
	"bytes"
	"fmt"
	"github.com/fanyeke/lsm-tree/memtable"
	"github.com/fanyeke/lsm-tree/wal"
	"io/fs"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

type Tree struct {
	conf          *Config                   // 配置信息
	dataLock      sync.RWMutex              // 读写数据需要加锁
	levelLocks    []sync.RWMutex            // 每一层的节点需要锁
	memtable      memtable.MemTable         // 读写数据的memtable
	rOnlyMemTable []*memTableCompactItem    // 只读的memtable
	walWriter     *wal.WALWriter            // 预写日志入口
	nodes         [][]*Node                 // 每一层的节点, 每层节点数量一致, 因此可以用二维数组表示
	memCompactC   chan *memTableCompactItem // 内存表达到阈值, 执行内存表到磁盘的溢写操作
	levelCompactC chan int                  // 某层sst文件大小达到阈值, 执行往下一层的归并操作
	stopc         chan struct{}             // lsm tree 停止的信号
	memTableIndex int                       // 与 wal 文件一一对应
	levelToSeq    []atomic.Int32            // 每一层的sst文件序号
}

// 读取sst文件, 还原出整棵树
func (t *Tree) constructTree() error {
	// 读取sst文件目录下的文件列表
	sstEntries, err := t.getSortedSSTEntries()
	if err != nil {
		return err
	}

	// 遍历每个sst文件, 将其加在为node添加到lsm tree的nodes内存切片中
	for _, sstEntry := range sstEntries {
		if err = t.loadNode(sstEntry); err != nil {
			return err
		}
	}

	return nil
}

func (t *Tree) constructMemTable() error {
	// 1. 读取wal文件目录下的文件列表
	raw, _ := os.ReadDir(path.Join(t.conf.Dir, "walfile"))

	// 文件除杂
	var wals []fs.DirEntry
	for _, entry := range raw {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".wal") {
			continue
		}

		wals = append(wals, entry)
	}

	// 2. 如果 wal 目录下没有文件, 则构建一个新的memtable
	if len(wals) == 0 {
		t.newMemTable()
		return nil
	}

	// 3. 读取 wal 文件, 还原出 memtable, 最晚的memtable作为读写memtable
	// 前置memtable作为只读memtable, 分别天机到内存 slice 和 channel中
	return t.restoreMemTable(wals)

}

// lsm tree 压缩调整协程
func (t *Tree) compact() {
	for {
		select {
		// 停止信号
		case <-t.stopc:
			return
		// 内存表达到阈值, 执行内存表到磁盘的溢写操作
		case memCompactItem := <-t.memCompactC:
			t.compactMemTable(memCompactItem)
		// 某层sst文件大小达到阈值, 执行往下一层的归并操作
		case level := <-t.levelCompactC:
			t.compactLevel(level)

		}
	}
}

func (t *Tree) Put(key, value []byte) error {
	// 1 加写锁
	t.dataLock.Lock()
	defer t.dataLock.Unlock()

	// 2.  数据预写入日志中, 防止宕机引起内存数据丢失
	if err := t.walWriter.Write(key, value); err != nil {
		return err
	}

	// 3. 数据写入跳表
	t.memtable.Put(key, value)

	// 4. 如果调表大小未达到level0层的阈值, 则直接返回
	// 溢写需要辅助元数据, 容量适当放大为 5/4 倍
	if uint64(t.memtable.Size()*5/4) < t.conf.SSTSize {
		return nil
	}

	// 5. 如果跳表数据量达到上限, 则切换跳表, 完成memtable模式切换
	t.refreshMemTableLocked()
	return nil
}

func (t *Tree) refreshMemTableLocked() {
	// 将跳表切换为只读模式
	oldItem := memTableCompactItem{
		walFile:  t.walFile(),
		memTable: t.memtable,
	}
	// 追加到只读跳表中
	t.rOnlyMemTable = append(t.rOnlyMemTable, &oldItem)
	t.walWriter.Close()
	go func() {
		// 给 compact 发送信号, 执行内存表到磁盘的溢写操作
		t.memCompactC <- &oldItem
	}()
	// 构造一个新的memtable, 并构建与之相应的wal文件
	t.memTableIndex++
	t.newMemTable()
}

func (t *Tree) Get(key []byte) ([]byte, bool, error) {
	//  加读锁
	t.dataLock.RLock()

	// 1. 首先读 active memtable
	value, ok := t.memtable.Get(key)
	if ok {
		t.dataLock.RUnlock()
		return value, true, nil
	}

	// 2. 读只读memtable
	for i := len(t.rOnlyMemTable) - 1; i >= 0; i-- {
		value, ok = t.rOnlyMemTable[i].memTable.Get(key)
		if ok {
			t.dataLock.RUnlock()
			return value, true, nil
		}
	}
	t.dataLock.RUnlock()

	// 3. 读 sstable level0 层(因为level0比较特殊)
	var err error
	t.levelLocks[0].RLock()
	// 从后往前扫描
	for i := len(t.nodes[0]) - 1; i >= 0; i-- {
		if value, ok, err = t.nodes[0][i].Get(key); err != nil {
			t.levelLocks[0].RUnlock()
			return nil, false, err
		}
		if ok {
			t.levelLocks[0].RUnlock()
			return value, true, nil
		}
	}
	t.levelLocks[0].RUnlock()

	// 4. 读 sstable 其他层
	for level := 1; level < len(t.nodes); level++ {
		// 根据给出的范围在索引中查找
		node, ok := t.levelBinarySearch(level, key, 0, len(t.nodes[level])-1)
		if !ok {
			// 不在当前层
			t.levelLocks[level].RUnlock()
			continue
		}
		// 在当前层
		if value, ok, err = node.Get(key); err != nil {
			t.levelLocks[level].RUnlock()
			return nil, false, err
		}
		if ok {
			t.levelLocks[level].RUnlock()
			return value, true, nil
		}
		t.levelLocks[level].RUnlock()
	}

	// 5. 未找到
	return nil, false, nil
}

func (t *Tree) newMemTable() {
	t.walWriter, _ = wal.NewWALWriter(t.walFile())
	t.memtable = t.conf.MemTableConstructor()
}

func (t *Tree) compactMemTable(memCompactItem *memTableCompactItem) {
	// 处理内存表到磁盘的溢写操作
	// 1. 内存表溢写到level0层
	t.flushMemTable(memCompactItem.memTable)

	// 2. 释放内存表
	t.dataLock.Lock()
	for i := 0; i < len(t.rOnlyMemTable); i++ {
		if t.rOnlyMemTable[i].memTable != memCompactItem.memTable {
			continue
		}
		t.rOnlyMemTable = t.rOnlyMemTable[i+1:]
	}
	t.dataLock.Unlock()

	// 3. 删除预写日志, 因为已经落盘到磁盘中
	_ = os.Remove(memCompactItem.walFile)
}

func (t *Tree) Close() {
	close(t.stopc)
	for i := 0; i < len(t.nodes); i++ {
		for j := 0; j < len(t.nodes[i]); j++ {
			t.nodes[i][j].Close()
		}
	}
}

func (t *Tree) restoreMemTable(wals []fs.DirEntry) error {
	// 1. wal 排序, 数据实时性也随之提高
	sort.Slice(wals, func(i, j int) bool {
		indexI := walFileToMemTableIndex(wals[i].Name())
		indexJ := walFileToMemTableIndex(wals[j].Name())
		return indexI < indexJ
	})

	// 2. 读取 wal 文件, 还原出 memtable, 最晚的memtable作为读写memtable
	for i := 0; i < len(wals); i++ {
		name := wals[i].Name()
		file := path.Join(t.conf.Dir, "walfile", name)

		// 2.1 读取 wal 文件, 构建与之对应的 walReader
		walReader, err := wal.NewWALReader(file)
		if err != nil {
			return err
		}
		defer walReader.Close()

		// 2.2 读取 wal 文件, 将其中的数据恢复到memtable中
		memtable := t.conf.MemTableConstructor()
		if err = walReader.RestoreToMemtable(memtable); err != nil {
			return err
		}

		if i == len(wals)-1 { // 如果是最后一个wal文件, 那么这个memtable就是读写memtable
			t.memtable = memtable
			t.memTableIndex = walFileToMemTableIndex(name)
			t.walWriter, _ = wal.NewWALWriter(file)
		} else { // 其他的memtable作为只读memtable
			memTableCompactItem := memTableCompactItem{
				walFile:  file,
				memTable: memtable,
			}

			t.rOnlyMemTable = append(t.rOnlyMemTable, &memTableCompactItem)
			t.memCompactC <- &memTableCompactItem
		}
	}
	return nil
}

// 将 memtable 的数据溢写落盘到 level0 层成为一个新的 sst 文件
func (t *Tree) flushMemTable(memTable memtable.MemTable) {
	// memtable 写到 level 0 层 sstable 中
	seq := t.levelToSeq[0].Load() + 1

	// 创建 sst writer
	sstWriter, _ := NewSSTWriter(t.sstFile(0, seq), t.conf)
	defer sstWriter.Close()

	// 遍历 memtable 写入数据到 sst writer
	for _, kv := range memTable.All() {
		sstWriter.Append(kv.Key, kv.Value)
	}

	// sstable 落盘
	size, blockToFilter, index := sstWriter.Finish()

	// 构造节点添加到 tree 的 node 中
	t.insertNode(0, seq, size, blockToFilter, index)
	// 尝试引发一轮 compact 操作
	t.tryTriggerCompact(0)
}
func (t *Tree) insertNode(level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	file := t.sstFile(level, seq)
	sstReader, _ := NewSSTReader(file, t.conf)

	t.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index)
}

// 插入一个 node 到指定 level 层
func (t *Tree) insertNodeWithReader(sstReader *SSTReader, level int, seq int32, size uint64, blockToFilter map[uint64][]byte, index []*Index) {
	file := t.sstFile(level, seq)
	// 记录当前 level 层对应的 seq 号（单调递增）
	t.levelToSeq[level].Store(seq)

	// 创建一个 lsm node
	newNode := NewNode(t.conf, file, sstReader, level, seq, size, blockToFilter, index)
	// 对于 level0 而言，只需要 append 插入 node 即可
	if level == 0 {
		t.levelLocks[0].Lock()
		t.nodes[level] = append(t.nodes[level], newNode)
		t.levelLocks[0].Unlock()
		return
	}

	// 对于 level1~levelk 层，需要根据 node 中 key 的大小，遵循顺序插入
	for i := 0; i < len(t.nodes[level])-1; i++ {
		// 遵循从小到大的遍历顺序，找到首个最小 key 比 newNode 最大 key 还大的 node，将 newNode 插入在其之前
		if bytes.Compare(newNode.End(), t.nodes[level][i+1].Start()) < 0 {
			t.levelLocks[level].Lock()
			t.nodes[level] = append(t.nodes[level][:i+1], t.nodes[level][i:]...)
			t.nodes[level][i+1] = newNode
			t.levelLocks[level].Unlock()
			return
		}
	}

	// 遍历完 level 层所有节点都还没插入 newNode，说明 newNode 是该层 key 值最大的节点，则 append 到最后即可
	t.levelLocks[level].Lock()
	t.nodes[level] = append(t.nodes[level], newNode)
	t.levelLocks[level].Unlock()
}

// 针对 level 层进行排序归并操作
func (t *Tree) compactLevel(level int) {
	// 获取到 level 和 level + 1 层内需要进行本次归并的节点
	pickedNodes := t.pickCompactNodes(level)

	// 插入到 level + 1 层对应的目标 sstWriter
	seq := t.levelToSeq[level+1].Load() + 1
	sstWriter, _ := NewSSTWriter(t.sstFile(level+1, seq), t.conf)
	defer sstWriter.Close()

	// 获取 level + 1 层每个 sst 文件的大小阈值
	sstLimit := t.conf.SSTSize * uint64(math.Pow10(level+1))
	// 获取本次排序归并的节点涉及到的所有 kv 数据
	pickedKVs := t.pickedNodesToKVs(pickedNodes)
	// 遍历每笔需要归并的 kv 数据
	for i := 0; i < len(pickedKVs); i++ {
		// 倘若新生成的 level + 1 层 sst 文件大小已经超限
		if sstWriter.Size() > sstLimit {
			// 将 sst 文件溢写落盘
			size, blockToFilter, index := sstWriter.Finish()
			// 将 sst 文件对应 node 插入到 lsm tree 内存结构中
			t.insertNode(level+1, seq, size, blockToFilter, index)
			// 构造一个新的 level + 1 层 sstWriter
			seq = t.levelToSeq[level+1].Load() + 1
			sstWriter, _ = NewSSTWriter(t.sstFile(level+1, seq), t.conf)
			defer sstWriter.Close()
		}

		// 将 kv 数据追加到 sstWriter
		sstWriter.Append(pickedKVs[i].Key, pickedKVs[i].Value)
		// 倘若这是最后一笔 kv 数据，需要负责把 sstWriter 溢写落盘并把对应 node 插入到 lsm tree 内存结构中
		if i == len(pickedKVs)-1 {
			size, blockToFilter, index := sstWriter.Finish()
			t.insertNode(level+1, seq, size, blockToFilter, index)
		}
	}

	// 移除这部分被合并的节点
	t.removeNodes(level, pickedNodes)

	// 尝试触发下一层的 compact 操作
	t.tryTriggerCompact(level + 1)
}

// 将一个 sst 文件作为一个 node 加载进入 lsm tree 的拓扑结构中
func (t *Tree) loadNode(sstEntry fs.DirEntry) error {
	// 创建 sst 文件对应的 reader
	sstReader, err := NewSSTReader(sstEntry.Name(), t.conf)
	if err != nil {
		return err
	}

	// 读取各 block 块对应的 filter 信息
	blockToFilter, err := sstReader.ReadFilter()
	if err != nil {
		return err
	}

	// 读取 index 信息
	index, err := sstReader.ReadIndex()
	if err != nil {
		return err
	}

	// 获取 sst 文件的大小，单位 byte
	size, err := sstReader.Size()
	if err != nil {
		return err
	}

	// 解析 sst 文件名，得知 sst 文件对应的 level 以及 seq 号
	level, seq := getLevelSeqFromSSTFile(sstEntry.Name())
	// 将 sst 文件作为一个 node 插入到 lsm tree 中
	t.insertNodeWithReader(sstReader, level, seq, size, blockToFilter, index)
	return nil
}
func (t *Tree) getSortedSSTEntries() ([]fs.DirEntry, error) {
	allEntries, err := os.ReadDir(t.conf.Dir)
	if err != nil {
		return nil, err
	}

	sstEntries := make([]fs.DirEntry, 0, len(allEntries))
	for _, entry := range allEntries {
		if entry.IsDir() {
			continue
		}

		if !strings.HasSuffix(entry.Name(), ".sst") {
			continue
		}

		sstEntries = append(sstEntries, entry)
	}

	sort.Slice(sstEntries, func(i, j int) bool {
		levelI, seqI := getLevelSeqFromSSTFile(sstEntries[i].Name())
		levelJ, seqJ := getLevelSeqFromSSTFile(sstEntries[j].Name())
		if levelI == levelJ {
			return seqI < seqJ
		}
		return levelI < levelJ
	})
	return sstEntries, nil
}

// 获取本轮 compact 流程涉及到的所有 kv 对. 这个过程中可能存在重复 k，保证只保留最新的 v
func (t *Tree) pickedNodesToKVs(pickedNodes []*Node) []*KV {
	// index 越小，数据越老. index 越大，数据越新
	// 所以使用大 index 的数据覆盖小 index 数据，以久覆新
	memtable := t.conf.MemTableConstructor()
	for _, node := range pickedNodes {
		kvs, _ := node.GetAll()
		for _, kv := range kvs {
			memtable.Put(kv.Key, kv.Value)
		}
	}

	// 借助 memtable 实现有序排列
	_kvs := memtable.All()
	kvs := make([]*KV, 0, len(_kvs))
	for _, kv := range _kvs {
		kvs = append(kvs, &KV{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	return kvs
}
func (t *Tree) walFile() string {
	return path.Join(t.conf.Dir, "walfile", fmt.Sprintf("%d.wal", t.memTableIndex))
}
func walFileToMemTableIndex(walFile string) int {
	rawIndex := strings.Replace(walFile, ".wal", "", -1)
	index, _ := strconv.Atoi(rawIndex)
	return index
}

func (t *Tree) sstFile(level int, seq int32) string {
	return fmt.Sprintf("%d_%d.sst", level, seq)
}
func (t *Tree) levelBinarySearch(level int, key []byte, start, end int) (*Node, bool) {
	if start > end {
		return nil, false
	}

	mid := start + (end-start)>>1
	if bytes.Compare(t.nodes[level][start].endKey, key) < 0 {
		return t.levelBinarySearch(level, key, mid+1, end)
	}

	if bytes.Compare(t.nodes[level][start].startKey, key) > 0 {
		return t.levelBinarySearch(level, key, start, mid-1)
	}

	return t.nodes[level][mid], true
}
func getLevelSeqFromSSTFile(file string) (level int, seq int32) {
	file = strings.Replace(file, ".sst", "", -1)
	splitted := strings.Split(file, "_")
	level, _ = strconv.Atoi(splitted[0])
	_seq, _ := strconv.Atoi(splitted[1])
	return level, int32(_seq)
}

// 移除所有完成 compact 流程的老节点
func (t *Tree) removeNodes(level int, nodes []*Node) {
	// 从 lsm tree 的 nodes 中移除老节点
outer:
	for k := 0; k < len(nodes); k++ {
		node := nodes[k]
		for i := level + 1; i >= level; i-- {
			for j := 0; j < len(t.nodes[i]); j++ {
				if node != t.nodes[i][j] {
					continue
				}

				t.levelLocks[i].Lock()
				t.nodes[i] = append(t.nodes[i][:j], t.nodes[i][j+1:]...)
				t.levelLocks[i].Unlock()
				continue outer
			}
		}
	}

	go func() {
		// 销毁老节点，包括关闭 sst reader，并且删除节点对应 sst 磁盘文件
		for _, node := range nodes {
			node.Destroy()
		}
	}()
}
func (t *Tree) tryTriggerCompact(level int) {
	// 最后一层不执行 compact 操作
	if level == len(t.nodes)-1 {
		return
	}

	var size uint64
	for _, node := range t.nodes[level] {
		size += node.size
	}

	if size <= t.conf.SSTSize*uint64(math.Pow10(level))*uint64(t.conf.SSTNumPerLevel) {
		return
	}

	go func() {
		t.levelCompactC <- level
	}()
}

// 获取本轮 compact 流程涉及到的所有节点，范围涵盖 level 和 level+1 层
func (t *Tree) pickCompactNodes(level int) []*Node {
	// 每次合并范围为当前层前一半节点
	startKey := t.nodes[level][0].Start()
	endKey := t.nodes[level][0].End()

	mid := len(t.nodes[level]) >> 1
	if bytes.Compare(t.nodes[level][mid].Start(), startKey) < 0 {
		startKey = t.nodes[level][mid].Start()
	}

	if bytes.Compare(t.nodes[level][mid].End(), endKey) > 0 {
		endKey = t.nodes[level][mid].End()
	}

	var pickedNodes []*Node
	// 将 level 层和 level + 1 层 和 [start,end] 范围有重叠的节点进行合并
	for i := level + 1; i >= level; i-- {
		for j := 0; j < len(t.nodes[i]); j++ {
			if bytes.Compare(endKey, t.nodes[i][j].Start()) < 0 || bytes.Compare(startKey, t.nodes[i][j].End()) > 0 {
				continue
			}

			// 所有范围有重叠的节点都追加到 list
			pickedNodes = append(pickedNodes, t.nodes[i][j])
		}
	}

	return pickedNodes
}
