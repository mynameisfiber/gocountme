package main

import (
	"log"
	"testing"
)

func TestUnionCard(t *testing.T) {
	SetupDB()
	query := `
{
    "method" : "cardinality",
    "set" : [
        {
            "method" : "union",
            "keys" : ["test1", "test2"]
        }
    ]
}
`
	log.Println(ParseQuery([]byte(query)))
	CloseDB()
}
