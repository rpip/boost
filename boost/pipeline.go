package boost

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ChimeraCoder/tokenbucket"
)

// Result holds object after being transformed by operation
type Result struct {
	Item Item
}

// Item represents decoded object as read from the input stream
type Item map[string]interface{}

// Transformer is the callback operation that tranforms the object
// it can clean data, apply business rules or validation, filter etc
type Transformer func(*Result) (*Result, error)

// Pipeline sets up the ETL (extract, transform, load) stages
type Pipeline struct {
	// input stream. default: stream STDIN
	source io.Reader
	// input stream. default: stream STDOUT
	sink         io.Writer
	transformers []Transformer
	log          *log.Logger
	verbose      bool
	numWorkers   int
	ctx          context.Context
	bucket       *tokenbucket.Bucket
}

// NewPipeline creates a new Pipeline with given source and output streams
func NewPipeline(ctx context.Context, source io.Reader, sink io.Writer,
	ops []Transformer, rate time.Duration, numWorkers int, verbose bool) *Pipeline {
	return &Pipeline{
		ctx:          ctx,
		source:       source,
		sink:         sink,
		transformers: ops,
		log:          log.New(os.Stderr, "", log.LstdFlags),
		numWorkers:   numWorkers,
		verbose:      verbose,
		// Allow a new action every 1/r seconds, with a maximum of 10 "in the bank"
		bucket: tokenbucket.NewBucket(rate, 10),
		// ms := int64(d2 / time.Millisecond)
	}
}

// Run does the actual stream processing
// it reads from source into IN channel. Spawn N workers to transform and stream data
// for each item, decodes the data into a map/struct
// and then runs transformations/filters etc and encode to json
// and finally, pushes to stdout
func (p *Pipeline) Run() {
	in := make(chan Item)
	defer close(in)

	var wg sync.WaitGroup
	wg.Add(p.numWorkers + 2)

	go func() {
		dec := json.NewDecoder(p.source)
		for {
			var v Item

			if err := dec.Decode(&v); err != nil {
				if err == io.EOF {
					return
				}
				p.debugf("Failed to decode object %s\n", err)
				return
			}
			// push decoded JSON to the channel
			in <- v
		}
	}()

	out := p.process(p.ctx, in, &wg)

	// spawn n OUT streamers
	for i := 0; i < p.numWorkers; i++ {
		go p.stream(out, &wg)
	}

	wg.Wait()
}

// stream pushes the data stream to the destination writer
func (p *Pipeline) stream(in chan *Result, wg *sync.WaitGroup) {
	enc := json.NewEncoder(p.sink)
	defer wg.Done()

	for result := range in {
		select {
		case <-p.ctx.Done():
			return
		default:
			if err := enc.Encode(&result.Item); err != nil {
				p.debugf("Failed to encode object %s\n", err)
			}
		}
	}
}

func (p *Pipeline) debugf(format string, args ...interface{}) {
	if p.verbose {
		p.log.Printf(format, args...)
	}
}

func (p *Pipeline) process(ctx context.Context, in chan Item, wg *sync.WaitGroup) chan *Result {
	// todo: buffer IN channel
	out := make(chan *Result)
	go func() {
		defer close(out)
		for item := range in {
			select {
			case <-ctx.Done():
				wg.Done()
				return
			default:
				// transform won't run until the bucket contains enough tokens
				<-p.bucket.SpendToken(1)
				// run filters etc
				// result, err := f(item)
				// if err != nil { maybe add to rejected set? }
				// otherwise, push to result
				if result, err := p.transform(item); err == nil {
					out <- result
				}
			}
		}
	}()
	return out
}

/// transfom
func (p *Pipeline) transform(obj Item) (*Result, error) {
	// run ops on obj and return transformed object or err
	// otherwise, return error to reject object
	res := &Result{Item: obj}
	for _, t := range p.transformers {
		if res, err := t(res); err != nil {
			return res, err
		}
	}
	return res, nil
}
