package ezdb

import (
	"errors"
	"fmt"
	"os"
	"strconv"

	"github.com/jmhodges/levigo"
)

var (
	// DEBUG is debug mode.
	// Set this flag to true to output debug messages from this package.
	DEBUG                             = false
	defCacheSize          int         = 1024 * 1024 * 16     // Default leveldb cache size.
	defDBFolderPermission os.FileMode = 0755                 // Default database dir permission
	errKeyNotExists                   = "key does not exist" // Key not exists error message.
)

// DB is a wrapper of levigo.DB.
type DB struct {
	LevigoDB *levigo.DB           // Instance of levigo.DB
	ro       *levigo.ReadOptions  // Read options for Get() of leveldb.
	roIt     *levigo.ReadOptions  // Read options for itarators of leveldb.
	wo       *levigo.WriteOptions // Write options for Put() of leveldb.
	cache    *levigo.Cache        // Cache of leveldb.
}

// GoThroughProcessor provides the interface to process leveldb record while go through the leveldb database.
type GoThroughProcessor interface {
	Process(k, v string) error
}

// Open opens a leveldb database.
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

	if err = os.MkdirAll(dbPath, defDBFolderPermission); err != nil {
		if DEBUG {
			fmt.Printf("os.MkDirAll(%v, %v) err: %v\n", dbPath, defDBFolderPermission, err)
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

// Close closes the leveldb database after use.
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
	// delete cache AFTER close leveldb or it will hang.
	// See cache in http://leveldb.googlecode.com/svn/trunk/doc/index.html
	if db.cache != nil {
		db.cache.Close()
	}
}

// Put is a wrapper for levigo.DB.Put().
func (db *DB) Put(key, value []byte) (err error) {
	return db.LevigoDB.Put(db.wo, key, value)
}

// Get is a wrapper for levigo.DB.Get().
func (db *DB) Get(key []byte) (value []byte, err error) {
	return db.LevigoDB.Get(db.ro, key)
}

// Delete is a wrapper for levigo.DB.Delete()
func (db *DB) Delete(key []byte) (err error) {
	return db.LevigoDB.Delete(db.wo, key)
}

// PutStr puts the key / value as string value.
func (db *DB) PutStr(key, value string) (err error) {
	return db.Put([]byte(key), []byte(value))
}

// KeyExist checks if key exists or not.
func (db *DB) KeyExist(key string) (exist bool, err error) {
	v, err := db.Get([]byte(key))
	if err != nil {
		return false, err
	}

	if v == nil {
		return false, nil
	}
	return true, nil
}

// GetStr gets the key / value as string value.
func (db *DB) GetStr(key string) (value string, err error) {
	v, err := db.Get([]byte(key))
	if v == nil {
		return "", errors.New(errKeyNotExists)
	}
	return string(v), err
}

// PutInt64 stores int64 as string in db. It should be used with Getint64().
func (db *DB) PutInt64(key string, value int64) (err error) {
	s := strconv.FormatInt(value, 10)
	return db.PutStr(key, s)
}

// GetInt64 get string value and convert it to int64. It should be used with PutInt64().
func (db *DB) GetInt64(key string) (value int64, err error) {
	s, err := db.GetStr(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(s, 10, 64)
}

// PutUint64 store uint64 as string in db. It should be used with GetUint64().
func (db *DB) PutUint64(key string, value uint64) (err error) {
	s := strconv.FormatUint(value, 10)
	return db.PutStr(key, s)
}

// GetUint64 get string value and convert it to uint64. It should be used with PutUInt64().
func (db *DB) GetUint64(key string) (value uint64, err error) {
	s, err := db.GetStr(key)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(s, 10, 64)
}

// DeleteStr delete the string key.
func (db *DB) DeleteStr(key string) (err error) {
	return db.Delete([]byte(key))
}

// NewIterator creates a new iterator of levigo.
func (db *DB) NewIterator() *levigo.Iterator {
	return db.LevigoDB.NewIterator(db.roIt)
}

// IsIteratorValidForGoThrough checks if current iterator is valid while go through the db.
func IsIteratorValidForGoThrough(it *levigo.Iterator, keyEnd string) bool {
	if keyEnd != "" {
		return it.Valid() && string(it.Key()) <= keyEnd
	}
	return it.Valid()
}

// GoThrough goes through the leveldb db and call the GoThroughProcessor.Process() to process data.
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
