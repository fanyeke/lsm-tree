package wal

import (
	"encoding/binary"
	"os"
)

// WALWriter 用于预写日志的写入
type WALWriter struct {
	file         string   // 预写日志的日志文件名, 是包含了目录在内的完整路径
	dest         *os.File // 实际的文件
	assistBuffer [30]byte // 辅助缓冲区
}

func NewWALWriter(file string) (*WALWriter, error) {
	// 打开文件, 如果文件不存在则创建
	dest, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &WALWriter{
		file: file,
		dest: dest,
	}, nil
}

func (w *WALWriter) Write(key, value []byte) error {
	// 首先将 key 和 value 的长度写入缓冲区
	n := binary.PutUvarint(w.assistBuffer[0:], uint64(len(key)))
	n += binary.PutUvarint(w.assistBuffer[n:], uint64(len(value)))

	// 将 key 和 value 写入缓冲区, 格式为: 长度 + key + value
	var buf []byte
	buf = append(buf, w.assistBuffer[:n]...)
	buf = append(buf, key...)
	buf = append(buf, value...)
	_, err := w.dest.Write(buf)
	return err
}

func (w *WALWriter) Close() {
	_ = w.dest.Close()
}
