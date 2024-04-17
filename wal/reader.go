package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"github.com/fanyeke/lsm-tree/memtable"
	"io"
	"os"
)

// WALReader 用于预写日志的读取
type WALReader struct {
	file   string        // 预写日志的日志文件名, 是包含了目录在内的完整路径
	src    *os.File      // 实际的文件
	reader *bufio.Reader // 读取器
}

func NewWALReader(file string) (*WALReader, error) {
	// 以只读模式打开文件, 如果文件不存在则创建
	src, err := os.OpenFile(file, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WALReader{
		file:   file,
		src:    src,
		reader: bufio.NewReader(src),
	}, nil
}

func (w *WALReader) RestoreToMemtable(memTable memtable.MemTable) error {
	// 读取WAL文件, 将其中的数据恢复到memtable中
	body, err := io.ReadAll(w.reader)
	if err != nil {
		return err
	}

	// 保证文件偏移量被重置
	defer func() {
		_, _ = w.src.Seek(0, io.SeekStart)
	}()

	// 将文件中读取到的内容解析为 key-value 对
	kvs, err := w.readAll(bytes.NewReader(body))
	if err != nil {
		return err
	}

	// 将 key-value 对写入 memtable
	for _, kv := range kvs {
		memTable.Put(kv.Key, kv.Value)
	}
	return nil
}

// 将文件中读取到的内容解析为 key-value 对
func (w *WALReader) readAll(reader *bytes.Reader) ([]*memtable.KV, error) {
	var kvs []*memtable.KV
	// 循环读取 key-value 对
	for {
		// 存储的格式为: 长度 + key + value
		// 读取 key 的长度
		keyLen, err := binary.ReadUvarint(reader)

		// 如果读取到文件末尾, 则退出循环
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		// 读取 value 的长度
		valueLen, err := binary.ReadUvarint(reader)
		if err != nil {
			return nil, err
		}

		// 读取 key
		key := make([]byte, keyLen)
		if _, err = io.ReadFull(reader, key); err != nil {
			return nil, err
		}

		// 读取 value
		value := make([]byte, valueLen)
		if _, err = io.ReadFull(reader, value); err != nil {
			return nil, err
		}

		kvs = append(kvs, &memtable.KV{
			Key:   key,
			Value: value,
		})
	}
	return kvs, nil
}

func (w *WALReader) Close() {
	w.reader.Reset(w.src)
	_ = w.src.Close()
}
