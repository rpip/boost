package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"time"

	"github.com/rpip/boost/boost"
)

var flagSet = flag.NewFlagSet("boostd", flag.ExitOnError)

func main() {
	// Process flags
	numWorkers := flagSet.Int("n", 4, "Max number of concurrent OUT streamers")
	verbose := flagSet.Bool("v", true, "Enable Verbose logging")
	rate := flagSet.Duration("r", time.Second/10, "burst rate")
	flagSet.Parse(os.Args[1:])

	// todo: handle signals. cleanup, cancel ops
	ctx := context.Background()
	transformers := []boost.Transformer{reject, clean, prefix, decorate}
	pipe := boost.NewPipeline(ctx, os.Stdin, os.Stdout, transformers, *rate, *numWorkers, *verbose)
	pipe.Run()
	os.Exit(0)
}

// transformers

// reject an object based on a value
func reject(res *boost.Result) (*boost.Result, error) {
	if res.Item["id"].(float64) == 557 {
		return res, errors.New("")
	}
	return res, nil
}

// remove a key from an object
func clean(res *boost.Result) (*boost.Result, error) {
	delete(res.Item, "lat")
	return res, nil
}

// prefix a key with a string
func prefix(res *boost.Result) (*boost.Result, error) {
	_prefix := "geo_"
	res.Item[_prefix+"lng"] = res.Item["lng"].(float64)
	delete(res.Item, "lng")
	return res, nil
}

// add a key value pair on an object
func decorate(res *boost.Result) (*boost.Result, error) {
	res.Item["foo"] = "bar"
	return res, nil
}
