package main

import _ "net/http/pprof"

import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"github.com/reusee/mmh3"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
)

var RequestChan chan RequestCommand

var (
	VERSION         = "0.1"
	showVersion     = flag.Bool("version", false, "print version string")
	httpAddress     = flag.String("http", ":8080", "HTTP service address (e.g., ':8080')")
	nWorkers        = flag.Int("nworkers", 1, "Number of workers interacting with the DB")
	defaultSize     = flag.Int("default-size", 1024, "Default size for KMin Value sets")
	leveldbLRUCache = flag.Int("lru-cache", 1<<16, "LRU Cache size for LevelDB")
	dblocation      = flag.String("db", ".", "Database location")
)

type correlationMatrixElement struct {
	Keys    [2]string `json:"keys"`
	Jaccard float64   `json:"jaccard"`
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}

	key := reqParams.Get("key")
	if key == "" {
		HttpError(w, 500, "MISSING_ARG_KEY")
		return
	}

	resultChan := make(chan Result)
	getRequest := GetRequest{
		Key:        key,
		ResultChan: resultChan,
	}
	RequestChan <- getRequest
	result := <-resultChan
	close(resultChan)
	HttpResponse(w, 200, result)
}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}

	key := reqParams.Get("key")
	if key == "" {
		HttpError(w, 500, "MISSING_ARG_KEY")
		return
	}

	resultChan := make(chan Result)
	deleteRequest := DeleteRequest{
		Key:        key,
		ResultChan: resultChan,
	}
	RequestChan <- deleteRequest
	result := <-resultChan
	close(resultChan)
	HttpResponse(w, 200, result)
}

func CardinalityHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}

	key := reqParams.Get("key")
	if key == "" {
		HttpError(w, 500, "MISSING_ARG_KEY")
		return
	}

	resultChan := make(chan Result)
	getRequest := GetRequest{
		Key:        key,
		ResultChan: resultChan,
	}
	RequestChan <- getRequest
	result := <-resultChan
	close(resultChan)
	if result.Error == nil {
		card := result.Data.Cardinality()
		HttpResponse(w, 200, card)
	} else {
		HttpResponse(w, 500, result.Error)
	}
}

func Hashify(orig []byte) uint64 {
	h := mmh3.Hash128(orig)
	return binary.LittleEndian.Uint64(h)
}

func AddHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}

	key := reqParams.Get("key")
	if key == "" {
		log.Println(r.URL.RawQuery)
		HttpError(w, 500, "MISSING_ARG_KEY")
		return
	}

	value := reqParams.Get("value")
	if value == "" {
		HttpError(w, 500, "MISSING_ARG_VALUE")
		return
	}
	hash := Hashify([]byte(value))

	result := addHash(key, hash)
	if result.Error == nil {
		HttpResponse(w, 200, "OK")
	} else {
		HttpResponse(w, 500, result.Error.Error())
	}
}

func AddHashHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}

	key := reqParams.Get("key")
	if key == "" {
		HttpError(w, 500, "MISSING_ARG_KEY")
		return
	}

	hash_raw := reqParams.Get("hash")
	if hash_raw == "" {
		HttpError(w, 500, "MISSING_ARG_HASH")
		return
	}
	hash, err := strconv.ParseUint(hash_raw, 10, 64)
	if err != nil {
		HttpResponse(w, 500, "INVALID_ARG_HASH")
		return
	}

	result := addHash(key, hash)
	if result.Error == nil {
		HttpResponse(w, 200, "OK")
	} else {
		HttpResponse(w, 500, result.Error.Error())
	}
}

func addHash(key string, hash uint64) Result {
	resultChan := make(chan Result)
	defer close(resultChan)
	addHashRequest := AddHashRequest{
		Key:        key,
		Hash:       hash,
		ResultChan: resultChan,
	}
	RequestChan <- addHashRequest
	return <-resultChan
}

func JaccardHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}

	if len(reqParams["key"]) != 2 {
		HttpError(w, 500, "MUST_PROVIDE_2_KEYS")
		return
	}

	key1 := reqParams["key"][0]
	if key1 == "" {
		HttpError(w, 500, "MISSING_ARG_KEY")
		return
	}

	key2 := reqParams["key"][1]
	if key2 == "" {
		HttpError(w, 500, "MISSING_ARG_KEY")
		return
	}

	resultChan := make(chan Result, 2)

	getRequest1 := GetRequest{
		Key:        key1,
		ResultChan: resultChan,
	}
	RequestChan <- getRequest1
	result1 := <-resultChan

	getRequest2 := GetRequest{
		Key:        key2,
		ResultChan: resultChan,
	}
	RequestChan <- getRequest2
	result2 := <-resultChan

	if result1.Error != nil {
		HttpResponse(w, 500, result1.Error.Error())
	} else if result2.Error != nil {
		HttpResponse(w, 500, result2.Error.Error())
	} else {
		jac := result1.Data.Jaccard(result2.Data)
		HttpResponse(w, 200, QueryResult{Num: jac})
	}
}

func CorrelationMatrixHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}

	if _, found := reqParams["key"]; !found {
		HttpError(w, 500, "MISSING_ARG_KEY")
		return
	}

	N := len(reqParams["key"])
	if N < 2 {
		HttpError(w, 500, "MUST_PROVIDE_2+_KEYS")
		return
	}

	resultChan := make(chan Result, N)
	defer close(resultChan)
	kmvs := make([]*Result, N)
	for _, key := range reqParams["key"] {
		getRequest := GetRequest{
			Key:        key,
			ResultChan: resultChan,
		}
		RequestChan <- getRequest
	}

	for i := 0; i < N; i++ {
		result := <-resultChan
		if result.Error != nil {
			HttpError(w, 500, result.Error.Error())
			return
		}
		kmvs[i] = &result
	}

	matrix := make([]correlationMatrixElement, 0, N*(N-1)/2)
	for i, r1 := range kmvs[:N-1] {
		for _, r2 := range kmvs[i+1:] {
			key := [2]string{r1.Key, r2.Key}
			j := r1.Data.Jaccard(r2.Data)
			matrix = append(matrix, correlationMatrixElement{key, j})
		}
	}

	HttpResponse(w, 200, matrix)
}

func QueryHandler(w http.ResponseWriter, r *http.Request) {
	reqParams, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		HttpError(w, 500, "INVALID_URI")
		return
	}

	query := reqParams.Get("q")
	if query == "" {
		HttpError(w, 500, "MISSING_ARG_Q")
		return
	}

	result, err := ParseQuery([]byte(query))
	if err != nil {
		HttpResponse(w, 500, err.Error())
		return
	} else {
		HttpResponse(w, 200, result)
		return
	}
	HttpResponse(w, 500, "SERVER_ERROR")
}

func ExitHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "OK")
	Exit()
}

func Exit() {
	close(RequestChan)
}

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Printf("gocountme: v%s\n", VERSION)
		return
	}

	if *defaultSize <= 0 {
		fmt.Printf("--default-size must be greater than 0\n")
		return
	}

	if _, err := os.Stat(*dblocation); err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Database location does not exist:", *dblocation)
			return
		}
	}

	log.Println("Opening levelDB")
	Default_KMinValues_Size = *defaultSize
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(*leveldbLRUCache))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open(*dblocation, opts)
	defer db.Close()

	if err != nil {
		log.Panicln(err)
	}

	RequestChan = make(chan RequestCommand, *nWorkers)
	workerWaitGroup := sync.WaitGroup{}
	log.Printf("Starting %d workers", *nWorkers)
	for i := 0; i < *nWorkers; i++ {
		go func(id int) {
			workerWaitGroup.Add(1)
			levelDBWorker(db, RequestChan)
			workerWaitGroup.Done()
		}(i)
	}

	http.HandleFunc("/get", GetHandler)
	http.HandleFunc("/delete", DeleteHandler)
	http.HandleFunc("/cardinality", CardinalityHandler)
	http.HandleFunc("/jaccard", JaccardHandler)
	http.HandleFunc("/correlation", CorrelationMatrixHandler)
	http.HandleFunc("/add", AddHandler)
	http.HandleFunc("/addhash", AddHashHandler)
	http.HandleFunc("/query", QueryHandler)
	http.HandleFunc("/exit", ExitHandler)

	log.Printf("Starting gocountme HTTP server on %s", *httpAddress)
	go func() {
		log.Fatal(http.ListenAndServe(*httpAddress, nil))
	}()

	workerWaitGroup.Wait()
}
