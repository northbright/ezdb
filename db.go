package ezdb

import (
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"strconv"
)

var (
	DefCacheSize int = 1024 * 1024 * 16
)

type DB struct {
	LevigoDB *levigo.DB
	ro       *levigo.ReadOptions
	roIt     *levigo.ReadOptions
	wo       *levigo.WriteOptions
	cache    *levigo.Cache
}

var DEBUG = true

var ERR_KEY_DOES_NOT_EXIST = "key does not exist"

func Open(path string, cacheSize int) (db *DB, err error) {
	db = new(DB)

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

	if db.LevigoDB, err = levigo.Open(path, opts); err != nil {
		fmt.Println(err)
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

func (db *DB) PutInt64(key string, value int64) (err error) {
	s := strconv.FormatInt(value, 10)
	return db.PutStr(key, s)
}

func (db *DB) GetInt64(key string) (value int64, err error) {
	s, err := db.GetStr(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

func (db *DB) PutUint64(key string, value uint64) (err error) {
	s := strconv.FormatUint(value, 10)
	return db.PutStr(key, s)
}

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
