package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"time"

	bolt "github.com/coreos/bbolt"
	"github.com/spf13/viper"
)

// Counter type
type Counter string

// These are counter names in the DB
const (
	CounterWALPos Counter = "WalPos"
)

// keyName for passing through viper
const keyName = "boltdb_handle"

// DB is an interface to a bolt db
type DB struct {
	bolt *bolt.DB
}

// GetHandleFromViper retrieves a handle from Viper
func GetHandleFromViper(v *viper.Viper) (*DB, error) {
	val := v.Get(keyName)
	if v == nil {
		return nil, fmt.Errorf("No key by name %s found in Viper", keyName)
	}

	if db, ok := val.(*DB); !ok {
		return nil, fmt.Errorf("Key value is not a pointer to DB but %T", val)
	} else {
		return db, nil
	}
}

// SetHandleInViper sets a handle in Viper
func SetHandleInViper(v *viper.Viper, db *DB) {
	v.Set(keyName, db)
}

// New returns an instance of DB
func New(file string) (c *DB, err error) {
	c = &DB{}
	c.bolt, err = bolt.Open(file, 0600, &bolt.Options{Timeout: 1 * time.Second})
	return
}

// BucketInit creates the bucket with provided name if it does not exist
func (c *DB) BucketInit(bucketName string) error {
	return c.bolt.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucketName))
		return err
	})
}

func (c *DB) getUint64(bucketName []byte, key []byte) (value uint64, err error) {
	err = c.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return fmt.Errorf("Bucket %s does not exist", bucketName)
		}

		v := b.Get(key)
		if v == nil {
			return nil
		}

		if l := len(v); l > binary.MaxVarintLen64 {
			return fmt.Errorf("Key is longer than %d bytes (%d)", binary.MaxVarintLen64, l)
		}

		if value, err = binary.ReadUvarint(bytes.NewReader(v)); err != nil {
			return fmt.Errorf("Unable to decode value (%+v): %s", v, err)
		}

		return nil
	})

	return
}

func (c *DB) setUint64(bucketName []byte, key []byte, value uint64) (err error) {
	err = c.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketName)
		if b == nil {
			return fmt.Errorf("Bucket %s does not exist", bucketName)
		}

		v := make([]byte, binary.MaxVarintLen64)
		binary.PutUvarint(v, value)

		return b.Put(key, v)
	})

	return
}

// CounterGet gets a value of the specified counter in the provided bucket
func (c *DB) CounterGet(bucketName string, counterName Counter) (value uint64, err error) {
	return c.getUint64([]byte(bucketName), []byte(counterName))
}

// CounterSet sets a value of the specified counter in the provided bucket
func (c *DB) CounterSet(bucketName string, counterName Counter, value uint64) (err error) {
	return c.setUint64([]byte(bucketName), []byte(counterName), value)
}

// Close waits for all TX to finish and closes DB
func (c *DB) Close() error {
	return c.bolt.Close()
}
