// Command corpus scans a directory tree of price-book CSV/TSV/TXT files with
// pkg/sniff's detect+propose pipeline and reports what it found: encoding
// mix, header-confidence distribution, the most common column mappings, and
// two backlogs for pkg/sniff's field-type registry — headers that look like
// a known field by value but were missed by name (missing-synonym
// candidates) and headers that match no known field at all (new-field
// candidates).
//
// It is read-only: detect and propose only, never commit, never write Turso.
// Running it against a real corpus does not touch the API server or its
// database; review the report, then use the /field-types API to accept
// whichever suggestions make sense.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/JayJamieson/csv-api/pkg/sniff"
)

var scanExts = map[string]bool{".csv": true, ".tsv": true, ".txt": true}

func main() {
	root := flag.String("root", ".", "root directory to scan for .csv/.tsv/.txt price books")
	out := flag.String("out", "corpus", "output path prefix; writes <out>-detail.jsonl and <out>-report.md/.json")
	workers := flag.Int("workers", runtime.GOMAXPROCS(0), "number of concurrent file scanners")
	stage := flag.Bool("stage", false, "also stage+promote each file in an in-memory DuckDB for typed/quarantine counts (much slower)")
	limit := flag.Int("limit", 0, "stop after N files (0 = no limit); use for a quick sanity pass before a full run")
	flag.Parse()

	files, err := discoverFiles(*root, *limit)
	if err != nil {
		log.Fatalf("discover files under %s: %v", *root, err)
	}
	if len(files) == 0 {
		log.Fatalf("no .csv/.tsv/.txt files found under %s", *root)
	}
	fmt.Fprintf(os.Stderr, "scanning %d files with %d workers (stage=%v)\n", len(files), *workers, *stage)

	detailFile, err := os.Create(*out + "-detail.jsonl")
	if err != nil {
		log.Fatalf("create detail file: %v", err)
	}
	defer detailFile.Close()
	detailW := bufio.NewWriter(detailFile)
	var detailMu sync.Mutex

	agg := newAggregator()
	opts := &sniff.ScanOptions{Registry: sniff.DefaultRegistry(), Stage: *stage}

	jobs := make(chan string)
	var wg sync.WaitGroup
	var done int64

	for i := 0; i < max(*workers, 1); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for path := range jobs {
				rep := scanOne(path, opts)
				agg.add(rep)

				detailMu.Lock()
				if b, err := json.Marshal(rep); err == nil {
					detailW.Write(b)
					detailW.WriteByte('\n')
				}
				detailMu.Unlock()

				n := atomic.AddInt64(&done, 1)
				if n%500 == 0 {
					fmt.Fprintf(os.Stderr, "  %d/%d\n", n, len(files))
				}
			}
		}()
	}
	for _, path := range files {
		jobs <- path
	}
	close(jobs)
	wg.Wait()

	if err := detailW.Flush(); err != nil {
		log.Fatalf("flush detail file: %v", err)
	}
	if err := writeReport(*out, agg); err != nil {
		log.Fatalf("write report: %v", err)
	}
	fmt.Fprintf(os.Stderr, "done: %d files scanned; report written to %s-report.md / .json\n", len(files), *out)
}

// scanOne wraps sniff.ScanFile with panic recovery so one pathological file
// can't kill a run that might be scanning thousands of them.
func scanOne(path string, opts *sniff.ScanOptions) (rep *sniff.FileReport) {
	defer func() {
		if r := recover(); r != nil {
			rep = &sniff.FileReport{File: path, HeaderIndex: -1, Error: fmt.Sprintf("panic: %v", r)}
		}
	}()
	rep, err := sniff.ScanFile(path, opts)
	if err != nil {
		return &sniff.FileReport{File: path, HeaderIndex: -1, Error: err.Error()}
	}
	return rep
}

func discoverFiles(root string, limit int) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // unreadable entry: skip it, don't abort the whole walk
		}
		if d.IsDir() {
			return nil
		}
		if scanExts[strings.ToLower(filepath.Ext(path))] {
			files = append(files, path)
			if limit > 0 && len(files) >= limit {
				return filepath.SkipAll
			}
		}
		return nil
	})
	return files, err
}
