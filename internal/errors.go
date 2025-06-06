package kvdb

import "errors"

// 打开或调用数据库方法时可能返回的错误
var (
	// ErrDatabaseNotOpen 当数据库实例在打开之前或关闭之后被访问时返回
	ErrDatabaseNotOpen = errors.New("database not open")

	// ErrDatabaseOpen 当打开一个已经打开的数据库时返回
	ErrDatabaseOpen = errors.New("database already open")

	// ErrInvalid 当数据库的两个元数据页都无效时返回
	// 通常发生在文件不是 kvdb 数据库时
	ErrInvalid = errors.New("invalid database")

	// ErrVersionMismatch 当数据文件是用不同版本的 kvdb 创建时返回
	ErrVersionMismatch = errors.New("version mismatch")

	// ErrChecksum 当任一元数据页校验和不匹配时返回
	ErrChecksum = errors.New("checksum error")

	// ErrTimeout 当数据库无法在传递给 Open() 的超时时间内获得独占锁时返回
	ErrTimeout = errors.New("timeout")
)

// 开始或提交事务时可能发生的错误
var (
	// ErrTxNotWritable 当在只读事务上执行写操作时返回
	ErrTxNotWritable = errors.New("tx not writable")

	// ErrTxClosed 当提交或回滚已经提交或回滚的事务时返回
	ErrTxClosed = errors.New("tx closed")

	// ErrDatabaseReadOnly 当在只读数据库上启动变更事务时返回
	ErrDatabaseReadOnly = errors.New("database is in read-only mode")
)

// 设置或删除值或存储桶时可能发生的错误
var (
	// ErrBucketNotFound 当尝试访问尚未创建的存储桶时返回
	ErrBucketNotFound = errors.New("bucket not found")

	// ErrBucketExists 当创建已存在的存储桶时返回
	ErrBucketExists = errors.New("bucket already exists")

	// ErrBucketNameRequired 当创建空名称存储桶时返回
	ErrBucketNameRequired = errors.New("bucket name required")

	// ErrKeyRequired 当插入零长度键时返回
	ErrKeyRequired = errors.New("key required")

	// ErrKeyTooLarge 当插入大于 MaxKeySize 的键时返回
	ErrKeyTooLarge = errors.New("key too large")

	// ErrValueTooLarge 当插入大于 MaxValueSize 的值时返回
	ErrValueTooLarge = errors.New("value too large")

	// ErrIncompatibleValue 当尝试在现有非存储桶键上创建或删除存储桶，
	// 或在现有存储桶键上创建或删除非存储桶键时返回
	ErrIncompatibleValue = errors.New("incompatible value")
)
