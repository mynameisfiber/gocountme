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
	kmv := NewKMinValues(1000)

	for i := 0; i < 2000; i++ {
		kmv.AddHash(GetRandHash())
	}

	card := kmv.Cardinality()
	relError := (card - 2000.0) / 2000.0
	theoryError := kmv.RelativeError()
	// We give an extra 2x wiggle room for the error because we really aren't
	// using an optimal hashing function for the problem
	if relError > theoryError*2 {
		t.Errorf("Relative error too high: %f instead of %f", relError, theoryError)
		t.FailNow()
	}
}

func TestKMinValuesUnion(t *testing.T) {
	kmv1 := NewKMinValues(1000)
	kmv2 := NewKMinValues(1000)

	for i := 0; i < 1000; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv1.AddHash(hash)
	}
	for i := 50; i < 1500; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv2.AddHash(hash)
	}

	kmv3 := kmv1.Union(kmv2)
	card := kmv3.Cardinality()
	relError := (card - 1500.0) / 1500.0
	theoryError := kmv3.RelativeError()
	// We give an extra 2x wiggle room for the error because we really aren't
	// using an optimal hashing function for the problem
	if relError > theoryError*2 {
		t.Errorf("Relative error too high: %f instead of %f", relError, theoryError)
		t.FailNow()
	}
}
