package main

import (
	"errors"
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/mynameisfiber/gocountme/kminvalues"
)

var (
	NoKeySpecified = errors.New("No Key supplied for db Request")
	NotImplemented = errors.New("Not Implemented")
)

type Result struct {
	Key   string
	Data  *kminvalues.KMinValues
	Error error
}

type RequestCommand interface {
	Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*kminvalues.KMinValues, error)
	WriteResult(result Result)
}

type GetRequest struct {
	Key        string
	ResultChan chan Result
}

type SetRequest struct {
	Key        string
	Kmv        *kminvalues.KMinValues
	ResultChan chan Result
}

type DeleteRequest struct {
	Key        string
	ResultChan chan Result
}

type AddHashRequest struct {
	Key        string
	Hash       uint64
	ResultChan chan Result
}

type ResizeRequest struct {
	Key        string
	NewSize    int
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

func (gr GetRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*kminvalues.KMinValues, error) {
	if gr.Key == "" {
		return nil, NoKeySpecified
	}

	data, err := database.Get(ro, []byte(gr.Key))
	if err != nil {
		return nil, err
	}

	kmv, err := kminvalues.KMinValuesFromBytes(data)
	return kmv, err
}

func (sr SetRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*kminvalues.KMinValues, error) {
	if sr.Key == "" {
		return nil, NoKeySpecified
	}

	keyBytes := []byte(sr.Key)
	err := database.Put(wo, keyBytes, sr.Kmv.Bytes())

	return sr.Kmv, err
}

func (dr DeleteRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*kminvalues.KMinValues, error) {
	if dr.Key == "" {
		return nil, NoKeySpecified
	}

	keyBytes := []byte(dr.Key)
	err := database.Delete(wo, keyBytes)

	return nil, err
}

func (ahr AddHashRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*kminvalues.KMinValues, error) {
	if ahr.Key == "" {
		return nil, NoKeySpecified
	}

	keyBytes := []byte(ahr.Key)

	data, err := database.Get(ro, keyBytes)
	if err != nil {
		return nil, err
	}

	kmv, err := kminvalues.KMinValuesFromBytes(data)
	if err != nil {
		if len(data) == 0 {
			kmv = kminvalues.NewKMinValues(*defaultSize)
		} else {
			return nil, err
		}
	}
	kmv.AddHash(ahr.Hash)

	err = database.Put(wo, keyBytes, kmv.Bytes())
	return kmv, err
}

func (rr ResizeRequest) Execute(database *levigo.DB, ro *levigo.ReadOptions, wo *levigo.WriteOptions) (*kminvalues.KMinValues, error) {
	// TODO: fix this
	return nil, fmt.Errorf("Not implemented")
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
