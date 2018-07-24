package boost

import (
	"bytes"
	"context"
	"log"
	"os"
	"testing"
	"time"
)

func TestPipeline(t *testing.T) {
	file, err := os.Open("testdata/ordered_driver_positions.json_dump")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var out bytes.Buffer

	numWorkers := 2
	verbose := false
	ctx := context.Background()
	transformers := []Transformer{decorate}
	p := NewPipeline(ctx, file, &out, transformers, time.Second/10, numWorkers, verbose)

	if p.numWorkers != numWorkers {
		t.Error("Expected numWorkers to be 2, got", p.numWorkers)
	}

	if p.verbose {
		t.Error("Expected verbose to be false, got", p.verbose)
	}
}

// add a key value pair on an object
func decorate(res *Result) (*Result, error) {
	res.Item["foo"] = "bar"
	return res, nil
}
