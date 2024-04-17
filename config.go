package lsm_tree

import (
	"github.com/fanyeke/lsm-tree/filter"
	"github.com/fanyeke/lsm-tree/memtable"
	"io/fs"
	"os"
	"path"
	"strings"
)

// Config 配置文件
type Config struct {
	Dir      string // sst文件存储目录
	MaxLevel int    // lsm tree 的最大层数

	SSTSize          uint64 // sst文件大小
	SSTNumPerLevel   int    // 每一层的sst文件数量
	SSTDataBlockSize int    // sst文件数据块大小
	SSTFooterSize    int    // sst文件footer大小

	Filter              filter.Filter                // 过滤器, 默认为布隆过滤器
	MemTableConstructor memtable.MemTableConstructor // 内存表构造器, 默认为跳表
}

func NewConfig(dir string, opts ...ConfigOption) (*Config, error) {
	c := Config{
		Dir:           dir, // sst文件存储目录
		SSTFooterSize: 32,  // sst文件footer大小, 对应 4 个 uint64, 共 32 字节
	}
	// 加载其他配置
	for _, opt := range opts {
		opt(&c)
	}
	// 兜底修复
	repaire(&c)
	// 检查配置
	return &c, c.check()
}

func (c *Config) check() error {
	// sstable 文件目录要确保存在
	if _, err := os.ReadDir(c.Dir); err != nil {
		_, ok := err.(*fs.PathError) // 判断错误类型
		// 如果不是文件不存在的错误
		if !ok || !strings.HasSuffix(err.Error(), "no such file or directory") {
			return err
		}
		// 如果是文件不存在的错误, 则创建目录
		if err = os.Mkdir(c.Dir, os.ModePerm); err != nil {
			return err
		}
	}

	// wal 文件目录确保存在
	walDir := path.Join(c.Dir, "walfile")
	if _, err := os.ReadDir(walDir); err != nil {
		_, ok := err.(*fs.PathError) // 判断错误类型
		// 如果不是文件不存在的错误
		if !ok || !strings.HasSuffix(err.Error(), "no such file or directory") {
			return err
		}
		// 如果是文件不存在的错误, 则创建目录
		if err = os.Mkdir(walDir, os.ModePerm); err != nil {
			return err
		}
	}

	return nil
}

// ConfigOption 配置项
type ConfigOption func(*Config)

// WithMaxLevel lsm tree的最大层数, 默认为 7
func WithMaxLevel(maxLevel int) ConfigOption {
	return func(c *Config) {
		c.MaxLevel = maxLevel
	}
}

// WithSSTSize level0 层每个 sst 文件的大小, 单位是 byte, 默认为 1 MB
// 每增加一层, sst 文件大小放大10 倍
func WithSSTSize(sstSize uint64) ConfigOption {
	return func(c *Config) {
		c.SSTSize = sstSize
	}
}

// WithSSTDataBlockSize sstable 文件数据块大小, 默认为 64 KB
func WithSSTDataBlockSize(sstDataBlockSize int) ConfigOption {
	return func(c *Config) {
		c.SSTDataBlockSize = sstDataBlockSize
	}
}

// WithSSTNumPreLevel 每层语气最多存放的 sst 文件数量, 默认为 10
func WithSSTNumPreLevel(sstNumPerLevel int) ConfigOption {
	return func(c *Config) {
		c.SSTNumPerLevel = sstNumPerLevel
	}
}

// WithFilter 注入过滤器的具体实现, 默认使用布隆过滤器
func WithFilter(filter filter.Filter) ConfigOption {
	return func(c *Config) {
		c.Filter = filter
	}
}

// WithMemTableConstructor 注入有序表构造器, 默认使用跳表
func WithMemTableConstructor(memTableConstructor memtable.MemTableConstructor) ConfigOption {
	return func(c *Config) {
		c.MemTableConstructor = memTableConstructor
	}
}

func repaire(c *Config) {
	// lsm tree 的最大层数
	if c.MaxLevel <= 1 {
		c.MaxLevel = 7
	}
	// level0 层每个 sstable 文件默认大小限制为 1MB.
	// 且每加深一层，sstable 文件大小限制阈值放大 10 倍.
	if c.SSTSize <= 0 {
		c.SSTSize = 1024 * 1024
	}
	// sstable 文件数据块大小, 默认为 16KB
	if c.SSTDataBlockSize <= 0 {
		c.SSTDataBlockSize = 16 * 1024 // 16 * 1024
	}
	// 每层最多存放的 sstable 文件数量, 默认为 10
	if c.SSTNumPerLevel <= 0 {
		c.SSTNumPerLevel = 10
	}
	// 过滤器默认为布隆过滤器
	if c.Filter == nil {
		c.Filter, _ = filter.NewBloomFilter(1024)
	}
	// 内存表构造器默认为跳表
	if c.MemTableConstructor == nil {
		c.MemTableConstructor = memtable.NewSkiplist
	}
}
