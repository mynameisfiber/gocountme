package main

import (
	"log"
	"testing"
)

func TestUnionCard(t *testing.T) {
	SetupDB()
	query := `
{
    "method" : "jaccard",
    "set" : [
        {
            "method" : "union",
            "keys" : ["test1", "test2"]
        }, 
        {
            "method" : "get",
            "keys" : ["test3"]
        }
    ]
}
`
	log.Println(ParseQuery([]byte(query)))
	CloseDB()
}
