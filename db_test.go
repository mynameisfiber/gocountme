package main

import (
	"github.com/bmizerany/assert"
	"github.com/jmhodges/levigo"
	"github.com/mynameisfiber/gocountme/kminvalues"
	"log"
	"math/rand"
	"testing"
)

func GetRandHash() uint64 {
	hash := uint64(rand.Int63())
	if rand.Intn(2) == 0 {
		hash += 1 << 63
	}
	return hash
}

func TestDB(t *testing.T) {
	SetupDB()
	defer CloseDB()

	key := "_GOTEST_TESTDB"
	resultChan := make(chan Result)

	clean := func() {
		delRequest := DeleteRequest{
			Key:        key,
			ResultChan: resultChan,
		}
		RequestChan <- delRequest
		<-resultChan
	}
	clean()
	defer clean()

	kmv := kminvalues.NewKMinValues(50)

	for i := 0; i < 100; i++ {
		kmv.AddHash(GetRandHash())
	}

	origCard := kmv.Cardinality()

	setRequest := SetRequest{
		Key:        key,
		Kmv:        kmv,
		ResultChan: resultChan,
	}
	RequestChan <- setRequest
	result := <-resultChan
	assert.Equal(t, result.Error, nil)

	for i := 0; i < 50; i++ {
		addHashRequest := AddHashRequest{
			Key:        key,
			Hash:       GetRandHash(),
			ResultChan: resultChan,
		}
		RequestChan <- addHashRequest
		result = <-resultChan
		assert.Equal(t, result.Error, nil)
	}

	newKmv := result.Data
	if newKmv.Cardinality() <= origCard {
		t.Errorf("Cardinality did not increase after 'AddHashRequest' operations")
		t.FailNow()
	}
}

func SetupDB() {
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(1024))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open("./db/tmp", opts)

	if err != nil {
		log.Panicln(err)
	}

	RequestChan = make(chan RequestCommand)
	go func() {
		levelDBWorker(db, RequestChan)
		db.Close()
	}()
}

func CloseDB() {
	close(RequestChan)
}
