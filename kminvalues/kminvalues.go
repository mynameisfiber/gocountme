package kminvalues

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"errors"
	"sort"
)

const bytesUint64 = 8
const hashMax = float64(1<<64 - 1)

func hashUint64ToBytes(hash uint64) []byte {
	hashBytes := new(bytes.Buffer)
	binary.Write(hashBytes, binary.BigEndian, hash)
	return hashBytes.Bytes()
}

func hashBytesToUint64(hashBytes []byte) uint64 {
	// TODO: error checking here
	var hash uint64
	hashReader := bytes.NewBuffer(hashBytes)
	binary.Read(hashReader, binary.BigEndian, &hash)
	return hash
}

func Union(others ...*KMinValues) *KMinValues {
	maxsize := smallestK(others...)
	maxlen := 0
	idxs := make([]int, len(others))
	for i, other := range others {
		if maxlen < other.Len() {
			maxlen = other.Len()
		}
		idxs[i] = other.Len() - 1
	}

	// We directly create a kminvalues object here so that we can have raw be
	// pre-initialized with nil values
	newkmv := &KMinValues{
		raw:     make([]byte, maxlen*bytesUint64, maxsize*bytesUint64),
		maxSize: maxsize,
	}

	var kmin, kminTmp []byte
	jmin := make([]int, 0, len(others))
	for i := maxlen - 1; i >= 0; i-- {
		kmin = nil
		jmin = jmin[:0]
		for j, other := range others {
			kminTmp = other.getHashBytes(idxs[j])
			if kminTmp != nil {
				if kmin == nil || kminTmp != nil && bytes.Compare(kmin, kminTmp) > 0 {
					kmin = kminTmp
					jmin = jmin[:0]
					jmin = append(jmin, j)
				} else if kmin != nil && bytes.Equal(kmin, kminTmp) {
					jmin = append(jmin, j)
				}
			}
		}
		for _, j := range jmin {
			idxs[j]--
		}
		if kmin != nil {
			newkmv.SetHash(i, kmin)
		}
	}
	return newkmv
}

func cardinality(maxSize int, kMin uint64) float64 {
	return float64(maxSize-1.0) * hashMax / float64(kMin)
}

func smallestK(others ...*KMinValues) int {
	minsize := others[0].maxSize
	for _, other := range others[1:] {
		if minsize > other.maxSize {
			minsize = other.maxSize
		}
	}
	return minsize
}

type KMinValues struct {
	raw     []byte
	maxSize int
}

func (kmv *KMinValues) MarshalJSON() ([]byte, error) {
	var buffer bytes.Buffer
	N := kmv.Len()
	fmt.Fprintf(&buffer, `{"k":%d, "data":[`, kmv.maxSize)
	for n := 0; n < N; n++ {
		if n == N-1 {
			fmt.Fprintf(&buffer, "%d]}", kmv.GetHash(n))
		} else {
			fmt.Fprintf(&buffer, "%d,", kmv.GetHash(n))
		}
	}
	return buffer.Bytes(), nil
}

func NewKMinValues(capacity int) *KMinValues {
	return &KMinValues{
		raw:     make([]byte, 0, capacity*bytesUint64),
		maxSize: capacity,
	}
}

func KMinValuesFromBytes(raw []byte) (*KMinValues, error) {
	if len(raw) == 0 {
		return nil, errors.New("error reading data")
	}
	buf := bytes.NewBuffer(raw)

	var maxSizeTmp uint64
	var maxSize int
	err := binary.Read(buf, binary.BigEndian, &maxSizeTmp)
	if err != nil {
		return nil, errors.New("error reading size")
	}
	maxSize = int(maxSizeTmp)

	kmv := &KMinValues{
		raw:     raw[bytesUint64:],
		maxSize: maxSize,
	}
	return kmv, nil
}

func (kmv *KMinValues) GetHash(i int) uint64 {
	hashBytes := kmv.raw[i*bytesUint64 : (i+1)*bytesUint64]
	return hashBytesToUint64(hashBytes)
}

func (kmv *KMinValues) getHashBytes(i int) []byte {
	if i < 0 || i >= kmv.Len() {
		return nil
	}
	return kmv.raw[i*bytesUint64 : (i+1)*bytesUint64]
}

func (kmv *KMinValues) Bytes() []byte {
	sizeBytes := make([]byte, bytesUint64, bytesUint64+len(kmv.raw))
	binary.BigEndian.PutUint64(sizeBytes, uint64(kmv.maxSize))
	result := append(sizeBytes, kmv.raw...)
	return result
}

func (kmv *KMinValues) Len() int { return len(kmv.raw) / bytesUint64 }

func (kmv *KMinValues) SetHash(i int, hash []byte) {
	ib := i * bytesUint64
	copy(kmv.raw[ib:], hash)
}

func (kmv *KMinValues) FindHash(hash uint64) int {
	hashBytes := hashUint64ToBytes(hash)
	return kmv.FindHashBytes(hashBytes)
}

func (kmv *KMinValues) FindHashBytes(hash []byte) int {
	idx, found := kmv.LocateHashBytes(hash)
	if found {
		return idx
	}
	return -1
}

func (kmv *KMinValues) LocateHashBytes(hash []byte) (int, bool) {
	found := sort.Search(kmv.Len(), func(i int) bool { return bytes.Compare(kmv.getHashBytes(i), hash) <= 0 })
	if found < kmv.Len() && bytes.Equal(kmv.getHashBytes(found), hash) {
		return found, true
	}
	return found, false
}

func (kmv *KMinValues) AddHash(hash uint64) bool {
	hashBytes := hashUint64ToBytes(hash)
	return kmv.AddHashBytes(hashBytes)
}

func (kmv *KMinValues) popSet(idx int, hash []byte) {
	ib := idx * bytesUint64
	copy(kmv.raw[:ib-bytesUint64], kmv.raw[bytesUint64:ib])
	copy(kmv.raw[ib-bytesUint64:], hash)
}

func (kmv *KMinValues) insert(idx int, hash []byte) {
	ib := idx * bytesUint64
	kmv.raw = append(kmv.raw, make([]byte, bytesUint64)...)
	copy(kmv.raw[ib+bytesUint64:], kmv.raw[ib:])
	copy(kmv.raw[ib:], hash)
}

// Adds a hash to the KMV and maintains the sorting of the values.
// Furthermore, we make sure that items we are inserting are unique by
// searching for them prior to insertion.  We wait to do this seach last
// because it is computationally expensive so we attempt to throw away the hash
// in every way possible before performing it.
func (kmv *KMinValues) AddHashBytes(hash []byte) bool {
	n := kmv.Len()
	if n >= kmv.maxSize {
		if bytes.Compare(kmv.getHashBytes(0), hash) < 0 {
			return false
		}
		idx, found := kmv.LocateHashBytes(hash)
		if !found {
			kmv.popSet(idx, hash)
		} else {
			return false
		}
	} else {
		idx, found := kmv.LocateHashBytes(hash)
		if !found {
			if cap(kmv.raw) == len(kmv.raw)+1 {
				kmv.increaseCapacity(len(kmv.raw) * 2)
			}
			kmv.insert(idx, hash)
		} else {
			return false
		}
	}
	return true
}

// Adds extra capacity to the underlying []uint64 array that stores the hashes
func (kmv *KMinValues) increaseCapacity(newcap int) error {
	N := cap(kmv.raw)
	if newcap < N {
		return errors.New("already at that capacity")
	}
	if newcap/bytesUint64 > kmv.maxSize {
		if N == kmv.maxSize*bytesUint64 {
			return errors.New("at max capacity")
		}
		newcap = kmv.maxSize * bytesUint64
	}
	newarray := make([]byte, len(kmv.raw), newcap)
	copy(newarray[:len(kmv.raw)], kmv.raw)
	kmv.raw = newarray
	return nil
}

func (kmv *KMinValues) Cardinality() float64 {
	if kmv.Len() < kmv.maxSize {
		return float64(kmv.Len())
	}
	return cardinality(kmv.maxSize, kmv.GetHash(0))
}

func (kmv *KMinValues) CardinalityIntersection(others ...*KMinValues) float64 {
	X, n := DirectSum(append(others, kmv)...)
	return float64(n) / float64(X.maxSize) * X.Cardinality()

}

func (kmv *KMinValues) CardinalityUnion(others ...*KMinValues) float64 {
	X, _ := DirectSum(append(others, kmv)...)
	return X.Cardinality()

}

func (kmv *KMinValues) Jaccard(others ...*KMinValues) float64 {
	X, n := DirectSum(append(others, kmv)...)
	return float64(n) / float64(X.maxSize)
}

// Returns a new KMinValues object is the union between the current and the
// given objects
func (kmv *KMinValues) Union(others ...*KMinValues) *KMinValues {
	return Union(append(others, kmv)...)
}

func (kmv *KMinValues) RelativeError() float64 {
	return math.Sqrt(2.0 / (math.Pi * float64(kmv.maxSize-2)))
}

func DirectSum(others ...*KMinValues) (*KMinValues, int) {
	n := 0
	X := Union(others...)
	// TODO: can we optimize this loop somehow?
	var found bool
	for i := 0; i < X.Len(); i++ {
		xHash := X.getHashBytes(i)
		found = true
		for _, other := range others {
			if other.FindHashBytes(xHash) < 0 {
				found = false
				break
			}
		}
		if found {
			n += 1
		}
	}
	return X, n
}
