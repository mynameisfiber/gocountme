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
	"fmt"
)

var (
	KeysAndSetError            = fmt.Errorf("Both keys and set are specified in query")
	CardinalitySingleTermError = fmt.Errorf("Method 'cardinality' can only take in one data source")
	SetNeedsKMV                = fmt.Errorf("Set specified with float output")
	InvalidMethod              = fmt.Errorf("Unrecognized method")
)

type Element struct {
	Method string    `json:"method"`
	Set    []Element `json:"method,omitempty"`
	Keys   []string  `json:"keys,omitempty"`
}

type QueryResult struct {
	Kmv *KMinValues `json:"set"`
	Num float64     `json:"result"`
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

	var data []KMinValues

	if len(e.Keys) != 0 {
		data = make([]KMinValues, len(e.Keys))
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
				data[0] = *result.Data
			} else {
				data[i] = *result.Data
				i++
			}
			if i == len(e.Keys) && &data[0] != nil {
				close(resultChan)
			}
		}
	} else if len(e.Set) != 0 {
		data = make([]KMinValues, len(e.Set))
		for i := 0; i < len(e.Set); i++ {
			tmp, err := parseQuery(&e.Set[i])
			if err != nil {
				return nil, err
			} else if tmp.Kmv == nil {
				return nil, SetNeedsKMV
			}
			data[i] = *(tmp.Kmv)
		}
	}

	if e.Method == "cardinality" {
		if len(data) != 1 {
			return nil, CardinalitySingleTermError
		}
		return &QueryResult{Num: data[0].Cardinality()}, nil
	} else if e.Method == "union" {
		tmp := data[0].Union(data[1:]...)
		return &QueryResult{Kmv: &tmp}, nil
	}
	return nil, InvalidMethod
}
