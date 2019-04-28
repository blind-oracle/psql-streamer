package db

import (
	"math/rand"
	"os"
	"strconv"
	"testing"

	bolt "github.com/coreos/bbolt"
	"github.com/stretchr/testify/assert"
)

var testDB = "./" + strconv.Itoa(rand.Intn(2<<60)) + ".bolt"

func TestCache(t *testing.T) {
	os.Remove(testDB)

	db, err := New(testDB)
	assert.Nil(t, err)

	err = db.BucketInit("test")
	assert.Nil(t, err)

	err = db.CounterSet("test", CounterWALPos, 0xDEADBEEF)
	assert.Nil(t, err)

	c, err := db.CounterGet("test", CounterWALPos)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0xDEADBEEF), c)

	c, err = db.CounterGet("test", "someNonExistantCounter")
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), c)

	// Non-existing buckets
	err = db.CounterSet("test2", CounterWALPos, 0xDEADBEEF)
	assert.NotNil(t, err)

	_, err = db.CounterGet("test2", CounterWALPos)
	assert.NotNil(t, err)

	// Shitty key
	err = db.bolt.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte("test")).Put([]byte(CounterWALPos), []byte("01234567890x"))
	})
	assert.Nil(t, err)

	c, err = db.CounterGet("test", CounterWALPos)
	assert.NotNil(t, err)

	err = db.Close()
	assert.Nil(t, err)
	os.Remove(testDB)
}

// Test datsabase insertion performance
func BenchmarkSet(b *testing.B) {
	os.Remove(testDB)

	db, _ := New(testDB)
	db.BucketInit("test")
	for i := 0; i < b.N; i++ {
		db.CounterSet("test", CounterWALPos, 0xDEADBEEF)
	}

	db.Close()
	os.Remove(testDB)
}

// Test datsabase read performance
func BenchmarkGet(b *testing.B) {
	os.Remove(testDB)

	db, _ := New(testDB)
	db.BucketInit("test")
	for i := 0; i < b.N; i++ {
		db.CounterGet("test", CounterWALPos)
	}

	db.Close()
	os.Remove(testDB)
}
