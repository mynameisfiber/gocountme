package main

import (
	"fmt"
	"github.com/bmizerany/assert"
	"hash/fnv"
	"math/rand"
	"testing"
)

func TestKMinValuesConstruct(t *testing.T) {
	kmv := NewKMinValues(50)
	assert.Equal(t, kmv.MaxSize, uint64(50))
	assert.Equal(t, len(kmv.Data), 0)
	assert.Equal(t, cap(kmv.Data), 50)
}

var (
	Fnv_Hasher = fnv.New64a()
)

func GetRandHash() int64 {
	hash := rand.Int63()
	if rand.Intn(2) == 0 {
		hash *= -1
	}
	return hash
}

func GetHash(key []byte) int64 {
	Fnv_Hasher.Write(key)
	hash := Fnv_Hasher.Sum64()
	Fnv_Hasher.Reset()
	return int64(hash)
}

func TestKMinValuesCardinality(t *testing.T) {
	kmv := NewKMinValues(50)

	for i := 0; i < 100; i++ {
		kmv.AddHash(GetRandHash())
	}

	card := kmv.Cardinality()
	relError := (card - 100.0) / 100.0
	theoryError := kmv.RelativeError()
	if relError > theoryError {
		t.Errorf("Relative error too high: %f instead of %f", relError, theoryError)
		t.FailNow()
	}
}

func TestKMinValuesUnion(t *testing.T) {
	kmv1 := NewKMinValues(50)
	kmv2 := NewKMinValues(50)

	for i := 0; i < 100; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv1.AddHash(hash)
	}
	for i := 50; i < 150; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv2.AddHash(hash)
	}

	kmv3 := kmv1.Union(kmv2)
	card := kmv3.Cardinality()
	relError := (card - 150.0) / 150.0
	theoryError := kmv3.RelativeError()
	if relError > theoryError {
		t.Errorf("Relative error too high: %f instead of %f", relError, theoryError)
		t.FailNow()
	}
}
