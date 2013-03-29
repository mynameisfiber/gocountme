package main

import (
    "github.com/jmhodges/levigo"
    "fmt"
    "log"
)

type Result struct {
    Data *KMinValues
    Err error
}

type Request struct {
    GetKey string

    SetKey string

    AddHash int64
    ResizeValue uint64

    Result chan *Result
}

var RequestChan chan *Request

var (
    OverSpecificRequest = fmt.Errorf("Request contains both a set and a get operations")
    UnderSpecificRequest = fmt.Errorf("Request contains neither a set or a get operations")
    NoSetOperation = fmt.Errorf("Requested set operation contains no operation")
)

func levelDBWorker(database *levigo.DB) error {
    ro := levigo.NewReadOptions()
    wo := levigo.NewWriteOptions()
    defer ro.Close()
    defer wo.Close()

    for request := range RequestChan {
        if (request.GetKey != "") && (request.SetKey != "") {
            request.Result <- &Result{Err: OverSpecificRequest}
            continue
        }
        if request.GetKey == "" && request.SetKey == "" {
            request.Result <- &Result{Err: UnderSpecificRequest}
            continue
        }

        if request.SetKey != "" {
            if (request.AddHash == 0 && request.ResizeValue == 0) {
                request.Result <- &Result{Err: NoSetOperation}
            }
            request.GetKey = request.SetKey
        }

        data, err := database.Get(ro, []byte(request.GetKey))
        if err != nil {
            request.Result <- &Result{Err: err}
            continue
        }

        kmin := KMinValuesFromBytes(data)
        
        if (request.SetKey != "") {
            if request.AddHash != 0 {
                kmin.AddHash(request.AddHash)
            }
            if request.ResizeValue != 0 {
                err := kmin.Resize(request.ResizeValue)
                if err != nil {
                    request.Result <- &Result{Err: err, Data: &kmin}
                    continue
                }
            }
            err = database.Put(wo, []byte(request.SetKey), kmin.Bytes())
            if err != nil {
                request.Result <- &Result{Err: err, Data: &kmin}
                continue
            }
        }
        
        request.Result <- &Result{Data: &kmin}
    }

    return nil
}

func main() {
    opts := levigo.NewOptions()
    opts.SetCache(levigo.NewLRUCache(1<<30))
    opts.SetCreateIfMissing(true)
    db, err := levigo.Open("./db/tmp", opts)
    defer db.Close()

    if err != nil {
        log.Panicln(err)
    }

    RequestChan = make(chan *Request, 1)
    go levelDBWorker(db)

    resultChan := make(chan *Result, 1)
    newRequest := Request{
        GetKey: "asdf",
        Result: resultChan,
    }
    RequestChan <- &newRequest
    result := <-resultChan
    log.Println(result.Data)

    newRequest = Request{
        SetKey: "asdf",
        AddHash: int64(3),
        Result: resultChan,
    }
    RequestChan <- &newRequest
    result = <-resultChan
    log.Println(result.Data)

    newRequest = Request{
        GetKey: "asdf",
        Result: resultChan,
    }
    RequestChan <- &newRequest
    result = <-resultChan
    log.Println(result.Data)

    newRequest = Request{
        SetKey: "asdf",
        ResizeValue: 4,
        Result: resultChan,
    }
    RequestChan <- &newRequest
    result = <-resultChan
    log.Println(result.Data)

    newRequest = Request{
        GetKey: "asdf",
        Result: resultChan,
    }
    RequestChan <- &newRequest
    result = <-resultChan
    log.Println(result.Data)
}
