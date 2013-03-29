package main

import (
	"bytes"
	"encoding/binary"
	"log"
	"sort"
)

var Hash_Max = 1<<31 - 1
var Default_Capacity = uint64(4) //1 << 10)

type KMinValues struct {
	Data    []int64
	MaxSize uint64
}

func NewKMinValues(capacity uint64) KMinValues {
	return KMinValues{
		Data:    make([]int64, 0, capacity),
		MaxSize: capacity,
	}
}

func KMinValuesFromBytes(raw []byte) KMinValues {
	if len(raw) == 0 {
		log.Println("empty byte array")
		return NewKMinValues(Default_Capacity)
	}
	buf := bytes.NewBuffer(raw)

	var maxSize uint64
	err := binary.Read(buf, binary.LittleEndian, &maxSize)
	if err != nil {
		log.Println("error reading size")
		return NewKMinValues(Default_Capacity)
	}

	s := uint64((len(raw) - 8) / 8)
	c := uint64(2*s + 1)
	if c > maxSize {
		c = maxSize
	}

	kmv := KMinValues{
		Data:    make([]int64, 0, c),
		MaxSize: maxSize,
	}

	var tmp int64
	for {
		err := binary.Read(buf, binary.LittleEndian, &tmp)
		if err != nil {
			break
		}
		if tmp != 0 {
			kmv.AddHash(tmp)
		}
	}
	return kmv
}

func (kmv *KMinValues) Bytes() []byte {
	// TODO: error checking here
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, uint64(kmv.MaxSize))
	if err != nil {
		log.Println("Error writing size:", err.Error())
	}

	for _, value := range kmv.Data {
		if value != 0 {
			err := binary.Write(buf, binary.LittleEndian, value)
			if err != nil {
				log.Println("binary.Write failed:", err)
			}
		}
	}
	return buf.Bytes()

}

func (kmv KMinValues) Len() int { return len(kmv.Data) }

func (kmv KMinValues) Less(i, j int) bool {
	// Reverse logic for reverse order
	return kmv.Data[i] > kmv.Data[j]
}

func (kmv KMinValues) Swap(i, j int) {
	kmv.Data[i], kmv.Data[j] = kmv.Data[j], kmv.Data[i]
}

// Adds a hash to the KMV and maintains the sorting of the values.
// Furthermore, we make sure that items we are inserting are unique by
// searching for them prior to insertion.  We wait to do this seach last
// because it is computationally expensive so we attempt to throw away the hash
// in every way possible before performing it.
func (kmv *KMinValues) AddHash(hash int64) bool {
	n := uint64(len(kmv.Data))
	if n == kmv.MaxSize {
		if kmv.Data[0] < hash {
			return false
		}
		found := sort.Search(len(kmv.Data), func(i int) bool { return kmv.Data[i] >= hash })
		if !(found < len(kmv.Data) && kmv.Data[found] == hash) {
			kmv.Data[0] = hash
		} else {
			return false
		}
	} else {
		found := sort.Search(len(kmv.Data), func(i int) bool { return kmv.Data[i] >= hash })
		if !(found < len(kmv.Data) && kmv.Data[found] == hash) {
			if cap(kmv.Data) == len(kmv.Data)+1 {
				kmv.increaseCapacity(n * 2)
			}
			kmv.Data = append(kmv.Data, hash)
		} else {
			return false
		}
	}
	sort.Sort(kmv)
	return true
}

// Resize the KMinValues datastructure by changing the MaxSize and resizing any
// data currently being stored in the structure.
func (kmv *KMinValues) Resize(newsize uint64) error {
	// TODO: This doesn't do what you expect... fix
	items := uint64(len(kmv.Data))
	if items > newsize {
		items = newsize
	}
	kmv.MaxSize = newsize
	return nil
}

// Adds extra capacity to the underlying []int64 array that stores the hashes
func (kmv *KMinValues) increaseCapacity(newcap uint64) bool {
	N := uint64(cap(kmv.Data))
	if newcap < N {
		return false
	}
	if newcap > kmv.MaxSize {
		if N == kmv.MaxSize {
			return false
		}
		newcap = kmv.MaxSize
	}
	newarray := make([]int64, len(kmv.Data), newcap)
	for i := 0; i < len(kmv.Data); i++ {
		newarray[i] = kmv.Data[i]
	}
	kmv.Data = newarray
	return true
}
