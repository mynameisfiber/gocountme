package main

import _ "net/http/pprof"

import (
	"flag"
	"fmt"
	"github.com/jmhodges/levigo"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

var RequestChan chan RequestCommand

var (
	VERSION         = "0.1"
	showVersion     = flag.Bool("version", false, "print version string")
	httpAddress     = flag.String("http", ":8080", "HTTP service address (e.g., ':8080')")
	nWorkers        = flag.Int("nworkers", 1, "Number of workers interacting with the DB")
	defaultSize     = flag.Int("default-size", 512, "Default size for KMin Value sets")
	leveldbLRUCache = flag.Int("lru-cache", 1<<10, "LRU Cache size for LevelDB")
)

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

func AddHandler(w http.ResponseWriter, r *http.Request) {
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
	hash, err := strconv.ParseInt(hash_raw, 10, 64)
	if err != nil {
		HttpResponse(w, 500, "INVALID_ARG_HASH")
		return
	}

	resultChan := make(chan Result)
	addHashRequest := AddHashRequest{
		Key:        key,
		Hash:       hash,
		ResultChan: resultChan,
	}
	RequestChan <- addHashRequest
	result := <-resultChan
	close(resultChan)
	if result.Error == nil {
		HttpResponse(w, 200, "OK")
	} else {
		HttpResponse(w, 500, err.Error())
	}
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

	log.Println("Opening levelDB")
	Default_KMinValues_Size = *defaultSize
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(*leveldbLRUCache))
	opts.SetCreateIfMissing(true)
	db, err := levigo.Open("./db/tmp", opts)
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
	http.HandleFunc("/add", AddHandler)
	http.HandleFunc("/query", QueryHandler)
	http.HandleFunc("/exit", ExitHandler)

	log.Printf("Starting gocountme HTTP server on %s", *httpAddress)
	go func() {
		log.Fatal(http.ListenAndServe(*httpAddress, nil))
	}()

	workerWaitGroup.Wait()
}
