// Copyright 2026 orobox
//
//This file is derived from code in GoogleCloudPlatform/pi-delivery.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/googlecloudplatform/pi-delivery/gen/index"
	"github.com/googlecloudplatform/pi-delivery/pkg/obj"
	"github.com/googlecloudplatform/pi-delivery/pkg/obj/gcs"
	"github.com/googlecloudplatform/pi-delivery/pkg/unpack"
	"github.com/sethvargo/go-retry"
	"go.uber.org/zap"
)

const (
	TOTAL_NUMBERS     = 1_000_000_00
	DIGITS_PER_NUMBER = 8
	CHUNK_SIZE        = 100_000_000
	WORKERS           = 128
	SEQUENCE          = "091424070412150411"
	MIN_MATCH         = 10
)

var logger *zap.SugaredLogger
var wg sync.WaitGroup

var sequencePairs []byte

type workerContextKey string

type task struct {
	start  int64
	n      int64
	cancel context.CancelFunc
}

func parseSequence(sequence string) ([]byte, error) {
	if len(sequence) == 0 {
		return nil, errors.New("sequence cannot be empty")
	}
	if len(sequence)%2 != 0 {
		return nil, fmt.Errorf("sequence length must be even, got %d", len(sequence))
	}

	pairs := make([]byte, len(sequence)/2)
	for i := 0; i < len(sequence); i += 2 {
		tens := sequence[i]
		ones := sequence[i+1]
		if tens < '0' || tens > '9' || ones < '0' || ones > '9' {
			return nil, fmt.Errorf("sequence must contain only digits: %q", sequence)
		}
		value := int(tens-'0')*10 + int(ones-'0')
		pairs[i/2] = byte(value % 26)
	}

	return pairs, nil
}

func pairMods(digits []byte) []byte {
	if len(digits) < 2 {
		return nil
	}

	mods := make([]byte, len(digits)-1)
	for i := 0; i+1 < len(digits); i++ {
		value := int(digits[i]-'0')*10 + int(digits[i+1]-'0')
		mods[i] = byte(value % 26)
	}

	return mods
}

func matchLength(mods []byte, start int, target []byte) int {
	matchedPairs := 0
	for i, want := range target {
		idx := start + 2*i
		if idx >= len(mods) || mods[idx] != want {
			break
		}
		matchedPairs++
	}

	return matchedPairs * 2
}

func process(ctx context.Context, task *task, logger *zap.SugaredLogger, client obj.Client) error {
	logger.Infof("processing task, start = %d, n = %v", task.start, task.n)

	rrd := index.Decimal.NewReader(ctx, client.Bucket(index.BucketName))
	defer rrd.Close()
	urd := unpack.NewReader(ctx, rrd)
	if _, err := urd.Seek(task.start, io.SeekStart); err != nil {
		return err
	}
	buf := make([]byte, task.n)
	n, err := io.ReadFull(urd, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return err
	}
	if n < 2 {
		return err
	}
	buf = buf[:n]

	overread := len(SEQUENCE) - 1
	if overread > 0 {
		extra := make([]byte, overread)
		m, extraErr := io.ReadFull(urd, extra)
		if extraErr != nil && !errors.Is(extraErr, io.EOF) && !errors.Is(extraErr, io.ErrUnexpectedEOF) {
			return extraErr
		}
		buf = append(buf, extra[:m]...)
	}

	mods := pairMods(buf)
	anchorPairs := MIN_MATCH / 2
	if anchorPairs == 0 {
		anchorPairs = 1
	}
	if anchorPairs > len(sequencePairs) {
		anchorPairs = len(sequencePairs)
	}
	anchorDigits := anchorPairs * 2
	searchLimit := n
	for off := 0; off < searchLimit; {
		if off+anchorDigits > len(buf) {
			break
		}

		matchLen := matchLength(mods, off, sequencePairs)
		if matchLen < anchorDigits {
			off++
			continue
		}

		fmt.Printf("%v, %v, %s\n",
			task.start+int64(off+1), matchLen, string(buf[off:off+matchLen]))
		off += matchLen
	}

	logger.Infof("digits processed: %d + %d digits",
		task.start, task.n)
	return nil
}

func worker(ctx context.Context, taskChan <-chan task, client obj.Client) {
	defer wg.Done()
	logger := logger.With("worker id", ctx.Value(workerContextKey("workerId")))
	defer logger.Sync()
	defer logger.Infow("worker exiting")

	logger.Info("worker started")
	b := retry.WithMaxRetries(3, retry.NewExponential(1*time.Second))
	for task := range taskChan {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := retry.Do(ctx, b, func(ctx context.Context) error {
			if err := process(ctx, &task, logger, client); err != nil {
				return retry.RetryableError(err)
			}
			return nil
		}); err != nil {
			logger.Errorw("process failed", "error", err)
			task.cancel()
		}
	}

}

func main() {
	l, _ := zap.NewDevelopment()
	defer l.Sync()
	zap.ReplaceGlobals(l)
	logger = l.Sugar()

	var err error
	sequencePairs, err = parseSequence(SEQUENCE)
	if err != nil {
		logger.Errorf("invalid sequence: %v", err)
		os.Exit(1)
	}
	if MIN_MATCH%2 != 0 {
		logger.Errorf("MIN_MATCH must be even, got %d", MIN_MATCH)
		os.Exit(1)
	}

	start := flag.Int64("s", 0, "Start offset")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	client, err := gcs.NewClient(ctx)
	if err != nil {
		logger.Errorf("couldn't create a GCS client: %v", err)
		os.Exit(1)
	}
	defer client.Close()

	taskChan := make(chan task, 256)

	for i := 0; i < WORKERS; i++ {
		wg.Add(1)
		ctx = context.WithValue(ctx, workerContextKey("workerId"), i)
		go worker(ctx, taskChan, client)
	}

	for i := *start; i < index.Decimal.TotalDigits(); i += CHUNK_SIZE {
		task := task{
			start:  i,
			n:      CHUNK_SIZE,
			cancel: cancel,
		}
		taskChan <- task
		if ctx.Err() != nil {
			logger.Errorf("context error: %v", ctx.Err())
			break
		}
	}
	close(taskChan)
	wg.Wait()
}