package kvdb

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// kvdb 支持的最大 mmap 大小
const maxMapSize = 0xFFFFFFFFFFFF // 256TB

// 创建数组指针时使用的大小
const maxAllocSize = 0x7FFFFFFF

func flock(db *DB, mode os.FileMode, exclusive bool, timeout time.Duration) error {
	var t time.Time
	for {
		// 如果超时则返回错误
		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return ErrTimeout
		}
		flag := syscall.LOCK_SH
		if exclusive {
			flag = syscall.LOCK_EX
		}

		// 尝试获取锁
		err := syscall.Flock(int(db.file.Fd()), flag|syscall.LOCK_NB)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}

		// 等待一段时间后重试
		time.Sleep(50 * time.Millisecond)
	}
}

func funlock(db *DB) error {
	return syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
}

func mmap(db *DB, sz int) error {
	// 将数据文件映射到内存
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return err
	}

	// 建议内核该 mmap 是随机访问的
	if err := madvise(b, syscall.MADV_RANDOM); err != nil {
		return fmt.Errorf("madvise: %s", err)
	}

	// 保存原始字节切片并转换为字节数组指针
	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

func munmap(db *DB) error {
	// 如果没有映射数据则忽略取消映射操作
	if db.dataref == nil {
		return nil
	}

	// 使用原始字节切片进行取消映射
	err := syscall.Munmap(db.dataref)
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return err
}

func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}

func fdatasync(db *DB) error {
	return db.file.Sync()
}
