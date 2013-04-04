package main

import (
	"fmt"
	"github.com/jmhodges/levigo"
)

var (
	NoKeySpecified = fmt.Errorf("No Key supplied for db Request")
	NotImplemented = fmt.Errorf("Not Implemented")
)

type Result struct {
	Key   string
	Data  *KMinValues
	Error error
}

type RequestCommand interface {
	Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*KMinValues, error)
	WriteResult(result Result)
}

type GetRequest struct {
	Key        string
	ResultChan chan Result
}

type SetRequest struct {
	Key        string
	Kmv        *KMinValues
	ResultChan chan Result
}

type DeleteRequest struct {
	Key        string
	ResultChan chan Result
}

type AddHashRequest struct {
	Key        string
	Hash       int64
	ResultChan chan Result
}

type ResizeRequest struct {
	Key        string
	NewSize    uint64
	ResultChan chan Result
}

func (gr GetRequest) WriteResult(result Result) {
	result.Key = gr.Key
	gr.ResultChan <- result
}
func (sr SetRequest) WriteResult(result Result) {
	result.Key = sr.Key
	sr.ResultChan <- result
}
func (dr DeleteRequest) WriteResult(result Result) {
	result.Key = dr.Key
	dr.ResultChan <- result
}
func (ahr AddHashRequest) WriteResult(result Result) {
	result.Key = ahr.Key
	ahr.ResultChan <- result
}
func (rr ResizeRequest) WriteResult(result Result) {
	result.Key = rr.Key
	rr.ResultChan <- result
}

func (gr GetRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*KMinValues, error) {
	if gr.Key == "" {
		return nil, NoKeySpecified
	}

	data, err := database.Get(ro, []byte(gr.Key))
	if err != nil {
		return nil, err
	}

	// TODO: add error handling in FromBytes
	kmv := KMinValuesFromBytes(data)
	return &kmv, nil
}

func (sr SetRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*KMinValues, error) {
	if sr.Key == "" {
		return nil, NoKeySpecified
	}

	keyBytes := []byte(sr.Key)
	err := database.Put(wo, keyBytes, sr.Kmv.Bytes())

	return sr.Kmv, err
}

func (dr DeleteRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*KMinValues, error) {
	if dr.Key == "" {
		return nil, NoKeySpecified
	}

	keyBytes := []byte(dr.Key)
	err := database.Delete(wo, keyBytes)

	return nil, err
}

func (ahr AddHashRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*KMinValues, error) {
	if ahr.Key == "" {
		return nil, NoKeySpecified
	}

	keyBytes := []byte(ahr.Key)

	data, err := database.Get(ro, keyBytes)
	if err != nil {
		return nil, err
	}

	kmv := KMinValuesFromBytes(data)
	kmv.AddHash(ahr.Hash)

	err = database.Put(wo, keyBytes, kmv.Bytes())
	return &kmv, err
}

func (rr ResizeRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*KMinValues, error) {
	if rr.Key == "" {
		return nil, NoKeySpecified
	}

	keyBytes := []byte(rr.Key)

	data, err := database.Get(ro, keyBytes)
	if err != nil {
		return nil, err
	}

	kmv := KMinValuesFromBytes(data)
	err = kmv.Resize(rr.NewSize)
	if err != nil {
		return &kmv, err
	}

	err = database.Put(wo, keyBytes, kmv.Bytes())
	return &kmv, err
}

func levelDBWorker(database *levigo.DB, requestChan chan RequestCommand) error {
	ro := levigo.NewReadOptions()
	wo := levigo.NewWriteOptions()
	defer ro.Close()
	defer wo.Close()

	for request := range requestChan {
		kmv, err := request.Execute(database, ro, wo)
		request.WriteResult(Result{
			Data:  kmv,
			Error: err,
		})
	}

	return nil
}
