package main

import (
	"fmt"
	"github.com/bmizerany/assert"
	"hash/fnv"
	"math"
	"math/rand"
	"testing"
)

func TestKMinValuesConstruct(t *testing.T) {
	kmv := NewKMinValues(50)
	assert.Equal(t, kmv.MaxSize, 50)
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

func TestKMinValuesCardinalityUnion(t *testing.T) {
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

	card := kmv1.CardinalityUnion(kmv2)
	relError := (card - 1500.0) / 1500.0
	theoryError := kmv1.RelativeError()
	// We give an extra 2x wiggle room for the error because we really aren't
	// using an optimal hashing function for the problem
	if relError > theoryError*2 {
		t.Errorf("Relative error too high: %f instead of %f", relError, theoryError)
		t.FailNow()
	}
}

func TestKMinValuesCardinalityIntersection(t *testing.T) {
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

	card := kmv1.CardinalityIntersection(kmv2)
	relError := (card - 950.0) / 950.0
	theoryError := kmv1.RelativeError()
	// We give an extra 2x wiggle room for the error because we really aren't
	// using an optimal hashing function for the problem
	if relError > theoryError*2 {
		t.Errorf("Relative error too high: %f instead of %f", relError, theoryError)
		t.FailNow()
	}
}

func TestKMinValuesJaccard(t *testing.T) {
	kmv1 := NewKMinValues(512)
	kmv2 := NewKMinValues(512)

	for i := 0; i < 4000; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv1.AddHash(hash)
	}
	for i := 0; i < 5000; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv2.AddHash(hash)
	}

	jaccard := kmv1.Jaccard(kmv2)
	relError := kmv1.RelativeError()
	obsError := 1.0 - jaccard*5.0/4.0
	if math.Abs(obsError) > relError*2 {
		t.Errorf("Jaccard error too large... got value of %f and needed value of %f (%0.2f%% error)", jaccard, 1.0/3.0, obsError*100.0)
		t.FailNow()
	}
}
