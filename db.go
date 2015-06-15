package ezdb

import (
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"os"
	"strconv"
)

var (
	DEBUG                              = false // Set this flag to true to output debug messages from this package.
	DefCacheSize           int         = 1024 * 1024 * 16
	DefDBFolderPermission  os.FileMode = 0755
	ERR_KEY_DOES_NOT_EXIST             = "key does not exist"
)

type DB struct {
	LevigoDB *levigo.DB
	ro       *levigo.ReadOptions
	roIt     *levigo.ReadOptions
	wo       *levigo.WriteOptions
	cache    *levigo.Cache
}

type GoThroughProcessor interface {
	Process(k, v string) error
}

func Open(dbPath string, cacheSize int) (db *DB, err error) {
	db = new(DB)

	if DEBUG {
		fmt.Printf("Open(): dbPath = %v, cacheSize = %v\n", dbPath, cacheSize)
	}

	if db.cache = levigo.NewLRUCache(cacheSize); db.cache == nil {
		err = errors.New("levigo.NewLRUCache() == nil")
		if DEBUG {
			fmt.Println(err)
		}
		return nil, err
	}
	opts := levigo.NewOptions()
	opts.SetCache(db.cache)
	opts.SetCreateIfMissing(true)

	if err = os.MkdirAll(dbPath, DefDBFolderPermission); err != nil {
		if DEBUG {
			fmt.Printf("os.MkDirAll(%v, %v) err: %v\n", dbPath, DefDBFolderPermission, err)
		}
		return nil, err
	}

	if db.LevigoDB, err = levigo.Open(dbPath, opts); err != nil {
		if DEBUG {
			fmt.Println(err)
		}
		return nil, err
	}

	db.ro = levigo.NewReadOptions()
	db.roIt = levigo.NewReadOptions()
	db.roIt.SetFillCache(false)
	db.wo = levigo.NewWriteOptions()

	return db, err
}

// Close an ezdb.DB database.
func (db *DB) Close() {
	if db == nil {
		return
	}

	if db.roIt != nil {
		db.roIt.Close()
	}

	if db.ro != nil {
		db.ro.Close()
	}

	if db.wo != nil {
		db.wo.Close()
	}

	if db.LevigoDB != nil {
		db.LevigoDB.Close()
	}
	// delete cache AFTER delete db or it will hang.
	// See cache in http://leveldb.googlecode.com/svn/trunk/doc/index.html
	if db.cache != nil {
		db.cache.Close()
	}
}

func (db *DB) Put(key, value []byte) (err error) {
	return db.LevigoDB.Put(db.wo, key, value)
}

func (db *DB) Get(key []byte) (value []byte, err error) {
	return db.LevigoDB.Get(db.ro, key)
}

func (db *DB) Delete(key []byte) (err error) {
	return db.LevigoDB.Delete(db.wo, key)
}

func (db *DB) PutStr(key, value string) (err error) {
	return db.Put([]byte(key), []byte(value))
}

func (db *DB) KeyExist(key string) (exist bool, err error) {
	v, err := db.Get([]byte(key))
	if err != nil {
		return false, err
	}

	if v == nil {
		return false, nil
	} else {
		return true, nil
	}
}

func (db *DB) GetStr(key string) (value string, err error) {
	v, err := db.Get([]byte(key))
	if v == nil {
		return "", errors.New(ERR_KEY_DOES_NOT_EXIST)
	} else {
		return string(v), err
	}
}

// Store int64 as string in db.
// Should be used with Getint64().
func (db *DB) PutInt64(key string, value int64) (err error) {
	s := strconv.FormatInt(value, 10)
	return db.PutStr(key, s)
}

// Get string value and convert it to int64.
// Should be used with PutInt64().
func (db *DB) GetInt64(key string) (value int64, err error) {
	s, err := db.GetStr(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

// Store uint64 as string in db.
// Should be used with GetUint64().
func (db *DB) PutUint64(key string, value uint64) (err error) {
	s := strconv.FormatUint(value, 10)
	return db.PutStr(key, s)
}

// Get string value and convert it to uint64.
// Should be used with PutUInt64().
func (db *DB) GetUint64(key string) (value uint64, err error) {
	s, err := db.GetStr(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(s, 10, 64)
}

func (db *DB) DeleteStr(key string) (err error) {
	return db.Delete([]byte(key))
}

func (db *DB) NewIterator() *levigo.Iterator {
	return db.LevigoDB.NewIterator(db.roIt)
}

func IsIteratorValidForGoThrough(it *levigo.Iterator, keyEnd string) bool {
	if keyEnd != "" {
		return it.Valid() && string(it.Key()) <= keyEnd
	} else {
		return it.Valid()
	}
}

func (db *DB) GoThrough(keyStart, keyEnd string, processor GoThroughProcessor) (err error) {
	it := db.NewIterator()
	defer it.Close()

	if keyStart != "" {
		it.Seek([]byte(keyStart))
	} else {
		it.SeekToFirst()
	}

	k := ""
	v := ""
	for ; IsIteratorValidForGoThrough(it, keyEnd); it.Next() {
		k = string(it.Key())
		v = string(it.Value())
		if err = processor.Process(k, v); err != nil {
			if DEBUG {
				fmt.Printf("processor.Process(%v, %v) err: %v\n", k, v, err)
			}
			return err
		}
	}

	if err := it.GetError(); err != nil {
		if DEBUG {
			fmt.Printf("it.GetError(): %s\n", err)
		}
		return err
	}

	return nil
}
