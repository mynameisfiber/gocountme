package main

import (
	"encoding/binary"
	"fmt"
	"github.com/bmizerany/assert"
	"github.com/reusee/mmh3"
	"math"
	"math/rand"
	"testing"
)

func TestKMinValuesConstruct(t *testing.T) {
	kmv := NewKMinValues(50)
	assert.Equal(t, kmv.MaxSize, 50)
	assert.Equal(t, len(kmv.Raw), 0)
	assert.Equal(t, cap(kmv.Raw), 50*BytesUint64)
}

func GetRandHash() uint64 {
	hash := uint64(rand.Int63())
	if rand.Intn(2) == 0 {
		hash += 1 << 63
	}
	return hash
}

func GetHash(key []byte) uint64 {
	h := mmh3.Hash128(key)
	return binary.LittleEndian.Uint64(h)
}

func TestKMinValuesBytes(t *testing.T) {
	kmv := NewKMinValues(1000)

	for i := 0; i < 5000; i++ {
		kmv.AddHash(GetRandHash())
	}

	bkmv := kmv.Bytes()
	kmv2 := KMinValuesFromBytes(bkmv)

	assert.Equal(t, kmv.MaxSize, kmv2.MaxSize)
	for i := 0; i < kmv.Len(); i++ {
		assert.Equal(t, kmv.GetHash(i), kmv2.GetHash(i))
	}

}

func TestKMinValuesSimple(t *testing.T) {
	kmv := NewKMinValues(5)

	kmv.AddHash(1)
	assert.Equal(t, kmv.GetHash(0), uint64(1))

	kmv.AddHash(4)
	kmv.AddHash(2)
	kmv.AddHash(3)
	kmv.AddHash(5)
	kmv.AddHash(6)

	assert.Equal(t, kmv.MaxSize, kmv.Len())

	for i := 0; i < kmv.Len(); i++ {
		assert.Equal(t, kmv.GetHash(i), uint64(kmv.Len()-i))
	}

	kmv.AddHash(2)
	kmv.AddHash(3)
	for i, k := range []uint64{5, 4, 3, 2, 1} {
		assert.Equal(t, kmv.GetHash(i), k)
	}
	assert.Equal(t, kmv.FindHash(3), 2)

	kmv.AddHash(0)
	assert.Equal(t, kmv.GetHash(4), uint64(0))

	for i, k := range []uint64{4, 3, 2, 1, 0} {
		assert.Equal(t, kmv.GetHash(i), k)
	}
}

func TestKMinValuesCardinality(t *testing.T) {
	kmv := NewKMinValues(1000)

	for i := 0; i < 5000; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv.AddHash(hash)
	}

	card := kmv.Cardinality()
	relError := (card - 5000.0) / 5000.0
	theoryError := kmv.RelativeError()
	if relError > theoryError {
		t.Errorf("Relative error too high: %f instead of %f", relError, theoryError)
		t.FailNow()
	}
}

func TestKMinValuesUnion(t *testing.T) {
	kmv1 := NewKMinValues(1000)
	kmv2 := NewKMinValues(1000)
	kmv3 := NewKMinValues(1000)

	for i := 0; i < 1500; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv1.AddHash(hash)
	}
	for i := 100; i < 1000; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv2.AddHash(hash)
	}
	for i := 400; i < 1500; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv3.AddHash(hash)
	}

	kmv4 := kmv1.Union(kmv2)
	kmv5 := kmv1.Union(kmv2, kmv3)

	for i := 0; i < kmv3.Len(); i++ {
		if kmv4.GetHash(i) != kmv1.GetHash(i) {
			t.Errorf("Union(1) broke on index %d / %d", i, kmv4.Len())
			t.FailNow()
		}
		if kmv5.GetHash(i) != kmv1.GetHash(i) {
			t.Errorf("Union(2) broke on index %d / %d", i, kmv4.Len())
			t.FailNow()
		}
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
	if relError > theoryError {
		t.Errorf("Relative error too high: %f instead of %f (ie: %f instead of %f)", relError, theoryError, card, 1500.)
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
	if relError > theoryError {
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
	for i := 1000; i < 5000; i++ {
		hash := GetHash([]byte(fmt.Sprintf("%d", i)))
		kmv2.AddHash(hash)
	}

	jaccard := kmv1.Jaccard(kmv2)
	relError := kmv1.RelativeError()
	obsError := 1.0 - jaccard*5.0/3.0
	if math.Abs(obsError) > relError {
		t.Errorf("Jaccard error too large... got value of %f and needed value of %f (%0.2f%% error)", jaccard, 3.0/5.0, obsError*100.0)
		t.FailNow()
	}
}
