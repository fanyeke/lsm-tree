package lsm_tree

import "github.com/fanyeke/lsm-tree/memtable"

type memTableCompactItem struct {
	walFile  string
	memTable memtable.MemTable
}
