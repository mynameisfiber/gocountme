package main

//
//    in order to request:
//
//        Jaccard( key1 u key2, key8 n key3 )
//
//    we request:
//
//    {
//        "method" : "jaccard",
//        "set" : [
//            {
//                "method" : "union",
//                "keys" : ["key1", "key2"],
//            },
//            {
//                "method" : "intersection",
//                "keys" : ["key8", "key3"],
//            },
//
//        ]
//    }
//
//    ============================================
//
//    in order to request:
//
//        Card( (key1 u key2 u key3) n key5)
//
//    we request:
//
//    {
//        "method" : "cardinality",
//        "set" : [
//            {
//                "method" : "intersection",
//                "set" : [
//                    {
//                        "method" : "union",
//                        "keys" : ["key1", "key2", "key3"]
//                    },
//                    {
//                        "method" : "get",
//                        "keys" : ["key5"]
//                    },
//                ]
//            },
//        ]
//    }
//////////////////////////////////////////////////////////////////////

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/mynameisfiber/gocountme/kminvalues"
	"strings"
)

var (
	KeysAndSetError            = errors.New("Both keys and set are specified in query")
	CardinalitySingleTermError = errors.New("Method 'cardinality' can only take in one data source")
	GetSingleTermError         = errors.New("Method 'get' can only take in one data source")
	SetNeedsKMV                = errors.New("Set specified with float output")
	InvalidMethod              = errors.New("Unrecognized method")
	MethodSetSize              = errors.New("Method requires 2+ sets or keys")
)

type Element struct {
	Method string    `json:"method"`
	Set    []Element `json:"set,omitempty"`
	Keys   []string  `json:"keys,omitempty"`
}

type QueryResult struct {
	Key   string                 `json:"key"`
	Kmv   *kminvalues.KMinValues `json:"set"`
	Num   float64                `json:"result"`
	Multi []*QueryResult         `json:"multi_result,omitempty"`
}

func ParseQuery(query_raw []byte) (*QueryResult, error) {
	query := Element{}
	err := json.Unmarshal(query_raw, &query)
	if err != nil {
		return nil, err
	}

	return parseQuery(&query)
}

func parseQuery(e *Element) (*QueryResult, error) {
	if len(e.Keys) != 0 && len(e.Set) != 0 {
		return nil, KeysAndSetError
	}

	var data []*kminvalues.KMinValues
	var keys []string

	if len(e.Keys) != 0 {
		data = make([]*kminvalues.KMinValues, len(e.Keys))
		resultChan := make(chan Result, len(e.Keys)+1)
		for _, key := range e.Keys {
			getRequest := GetRequest{
				Key:        key,
				ResultChan: resultChan,
			}
			RequestChan <- getRequest
		}
		i := 1
		for result := range resultChan {
			if result.Key == e.Keys[0] {
				data[0] = result.Data
			} else {
				data[i] = result.Data
				i++
			}
			if i == len(e.Keys) && &data[0] != nil {
				close(resultChan)
			}
		}
		keys = e.Keys
	} else if len(e.Set) != 0 {
		data = make([]*kminvalues.KMinValues, len(e.Set))
		keys = make([]string, len(e.Set))
		for i := 0; i < len(e.Set); i++ {
			tmp, err := parseQuery(&e.Set[i])
			if err != nil {
				return nil, err
			} else if tmp.Kmv == nil {
				return nil, SetNeedsKMV
			}
			data[i] = tmp.Kmv
			keys[i] = tmp.Key
		}
	}

	if e.Method == "cardinality" {
		if len(data) != 1 {
			return nil, CardinalitySingleTermError
		}
		return &QueryResult{
			Key: fmt.Sprintf("||%s||", keys[0]),
			Num: data[0].Cardinality(),
		}, nil
	} else if e.Method == "get" {
		if len(data) != 1 {
			return nil, CardinalitySingleTermError
		}
		return &QueryResult{
			Key: keys[0],
			Kmv: data[0],
		}, nil
	} else if e.Method == "union" {
		if len(data) < 2 {
			return nil, MethodSetSize
		}
		tmp := data[0].Union(data[1:]...)
		return &QueryResult{
			Key: strings.Join(keys, " u "),
			Kmv: tmp,
		}, nil
	} else if e.Method == "jaccard" {
		if len(data) < 2 {
			return nil, MethodSetSize
		}
		tmp := data[0].Jaccard(data[1:]...)
		return &QueryResult{
			Key: fmt.Sprintf("Jaccard(%s)", strings.Join(keys, ", ")),
			Num: tmp,
		}, nil
	} else if e.Method == "cardinality_intersection" {
		if len(data) < 2 {
			return nil, MethodSetSize
		}
		tmp := data[0].CardinalityIntersection(data[1:]...)
		return &QueryResult{
			Key: fmt.Sprintf("||%s||", strings.Join(keys, " n ")),
			Num: tmp,
		}, nil
	} else if e.Method == "cardinality_union" {
		if len(data) < 2 {
			return nil, MethodSetSize
		}
		tmp := data[0].CardinalityUnion(data[1:]...)
		return &QueryResult{
			Key: fmt.Sprintf("||%s||", strings.Join(keys, " u ")),
			Num: tmp,
		}, nil
	} else if e.Method == "correlation" {
		if len(data) < 2 {
			return nil, MethodSetSize
		}

		N := len(data)
		correlation := make([]*QueryResult, 0, N*(N-1)/2)
		for i, r1 := range data[:N-1] {
			for j, r2 := range data[i+1:] {
				correlation = append(correlation, &QueryResult{
					Key: fmt.Sprintf("Jaccard(%s, %s)", keys[i], keys[j+i+1]),
					Num: r1.Jaccard(r2),
				})
			}
		}
		return &QueryResult{
			Key:   fmt.Sprintf("Corr(%s)", strings.Join(keys, ", ")),
			Multi: correlation,
		}, nil
	}
	return nil, InvalidMethod
}
