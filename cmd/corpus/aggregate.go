package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/JayJamieson/csv-api/pkg/sniff"
)

const maxExamples = 5

// aggregator accumulates per-file scan results into the counts the corpus
// report is built from. All methods are safe for concurrent use; one
// aggregator is shared by every worker goroutine.
type aggregator struct {
	mu sync.Mutex

	totalFiles int
	errors     int

	confidence map[string]int // header confidence bucket -> file count
	encoding   map[string]int // source encoding -> file count

	// "norm_header -> field": how often a mapping was proposed, ranked by
	// file count. This is the "most common mappings" list.
	mappingCounts map[string]int

	// missing-synonym candidates: keyed by "norm_header|suggested_field".
	synonymCand map[string]*synonymBucket

	// new-field candidates: unmapped headers with no confident value-shape
	// match either, clustered by normalized header name.
	newFieldCand map[string]*newFieldBucket

	// files by failure/no-header reason.
	errorCatalogue map[string]*exampleBucket
}

type synonymBucket struct {
	Field      string
	Count      int
	ScoreTotal float64
	Examples   []string
}

type newFieldBucket struct {
	Count    int
	Samples  []string
	Examples []string
}

type exampleBucket struct {
	Count    int
	Examples []string
}

func newAggregator() *aggregator {
	return &aggregator{
		confidence:     map[string]int{},
		encoding:       map[string]int{},
		mappingCounts:  map[string]int{},
		synonymCand:    map[string]*synonymBucket{},
		newFieldCand:   map[string]*newFieldBucket{},
		errorCatalogue: map[string]*exampleBucket{},
	}
}

func (a *aggregator) add(rep *sniff.FileReport) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.totalFiles++

	if rep.Error != "" {
		a.errors++
		a.bumpError(rep.Error, rep.File)
		return
	}
	if rep.Encoding != "" {
		a.encoding[rep.Encoding]++
	}

	if rep.HeaderIndex < 0 {
		a.bumpError("no header found", rep.File)
		return
	}
	a.confidence[rep.Confidence]++

	for _, col := range rep.Columns {
		switch {
		case col.Proposed != "":
			key := col.Norm + " -> " + col.Proposed
			a.mappingCounts[key]++
		case col.SuggestField != "":
			key := col.Norm + "|" + col.SuggestField
			b, ok := a.synonymCand[key]
			if !ok {
				b = &synonymBucket{Field: col.SuggestField}
				a.synonymCand[key] = b
			}
			b.Count++
			b.ScoreTotal += col.SuggestScore
			if len(b.Examples) < maxExamples {
				b.Examples = append(b.Examples, rep.File)
			}
		default:
			b, ok := a.newFieldCand[col.Norm]
			if !ok {
				b = &newFieldBucket{}
				a.newFieldCand[col.Norm] = b
			}
			b.Count++
			if len(b.Samples) == 0 {
				b.Samples = col.Samples
			}
			if len(b.Examples) < maxExamples {
				b.Examples = append(b.Examples, rep.File)
			}
		}
	}
}

func (a *aggregator) bumpError(reason, file string) {
	b, ok := a.errorCatalogue[reason]
	if !ok {
		b = &exampleBucket{}
		a.errorCatalogue[reason] = b
	}
	b.Count++
	if len(b.Examples) < maxExamples {
		b.Examples = append(b.Examples, file)
	}
}

// --- report rendering ---

type rankedEntry struct {
	Key   string
	Count int
}

func rankByCount(m map[string]int) []rankedEntry {
	out := make([]rankedEntry, 0, len(m))
	for k, v := range m {
		out = append(out, rankedEntry{k, v})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Key < out[j].Key
	})
	return out
}

func writeReport(outPrefix string, a *aggregator) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err := writeJSONReport(outPrefix+"-report.json", a); err != nil {
		return err
	}
	return writeMarkdownReport(outPrefix+"-report.md", a)
}

// jsonReport is a plain-data snapshot of the aggregator for the machine-
// readable report; the -emit-suggestions consumer (or a human reviewing
// before calling POST /field-types) reads this rather than the JSONL detail.
type jsonReport struct {
	TotalFiles         int                       `json:"total_files"`
	Errors             int                       `json:"errors"`
	ConfidenceDist     map[string]int            `json:"confidence_distribution"`
	EncodingDist       map[string]int            `json:"encoding_distribution"`
	TopMappings        []rankedEntry             `json:"top_mappings"`
	MissingSynonyms    []jsonSynonymCandidate    `json:"missing_synonym_candidates"`
	NewFieldCandidates []jsonNewFieldCandidate   `json:"new_field_candidates"`
	ErrorCatalogue     []jsonErrorCatalogueEntry `json:"error_catalogue"`
}

type jsonSynonymCandidate struct {
	Header   string   `json:"header"`
	Field    string   `json:"suggest_field"`
	Count    int      `json:"file_count"`
	AvgScore float64  `json:"avg_score"`
	Examples []string `json:"examples"`
}

type jsonNewFieldCandidate struct {
	Header   string   `json:"header"`
	Count    int      `json:"file_count"`
	Samples  []string `json:"samples"`
	Examples []string `json:"examples"`
}

type jsonErrorCatalogueEntry struct {
	Reason   string   `json:"reason"`
	Count    int      `json:"file_count"`
	Examples []string `json:"examples"`
}

func writeJSONReport(path string, a *aggregator) error {
	rep := jsonReport{
		TotalFiles:     a.totalFiles,
		Errors:         a.errors,
		ConfidenceDist: a.confidence,
		EncodingDist:   a.encoding,
		TopMappings:    rankByCount(a.mappingCounts),
	}
	for _, e := range rankByCount(countsFromSynonym(a.synonymCand)) {
		parts := strings.SplitN(e.Key, "|", 2)
		b := a.synonymCand[e.Key]
		avg := 0.0
		if b.Count > 0 {
			avg = b.ScoreTotal / float64(b.Count)
		}
		rep.MissingSynonyms = append(rep.MissingSynonyms, jsonSynonymCandidate{
			Header: parts[0], Field: b.Field, Count: b.Count, AvgScore: avg, Examples: b.Examples,
		})
	}
	for _, e := range rankByCount(countsFromNewField(a.newFieldCand)) {
		b := a.newFieldCand[e.Key]
		rep.NewFieldCandidates = append(rep.NewFieldCandidates, jsonNewFieldCandidate{
			Header: e.Key, Count: b.Count, Samples: b.Samples, Examples: b.Examples,
		})
	}
	for _, e := range rankByCount(countsFromErrors(a.errorCatalogue)) {
		b := a.errorCatalogue[e.Key]
		rep.ErrorCatalogue = append(rep.ErrorCatalogue, jsonErrorCatalogueEntry{
			Reason: e.Key, Count: b.Count, Examples: b.Examples,
		})
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

func countsFromSynonym(m map[string]*synonymBucket) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v.Count
	}
	return out
}

func countsFromNewField(m map[string]*newFieldBucket) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v.Count
	}
	return out
}

func countsFromErrors(m map[string]*exampleBucket) map[string]int {
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v.Count
	}
	return out
}

func writeMarkdownReport(path string, a *aggregator) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fmt.Fprintf(f, "# Corpus scan report\n\n")
	fmt.Fprintf(f, "- Files scanned: %d\n", a.totalFiles)
	fmt.Fprintf(f, "- Files with errors / no header: %d\n\n", a.errors)

	fmt.Fprintf(f, "## Header confidence distribution\n\n")
	for _, e := range rankByCount(a.confidence) {
		fmt.Fprintf(f, "- %s: %d\n", e.Key, e.Count)
	}

	fmt.Fprintf(f, "\n## Source encoding distribution\n\n")
	for _, e := range rankByCount(a.encoding) {
		fmt.Fprintf(f, "- %s: %d\n", e.Key, e.Count)
	}

	fmt.Fprintf(f, "\n## Most common mappings\n\n")
	fmt.Fprintf(f, "Normalized header -> proposed field, ranked by file count.\n\n")
	for _, e := range rankByCount(a.mappingCounts) {
		fmt.Fprintf(f, "- `%s` (%d files)\n", e.Key, e.Count)
	}

	fmt.Fprintf(f, "\n## Missing-synonym candidates\n\n")
	fmt.Fprintf(f, "Unmapped by name, but values clearly match a known field. "+
		"Highest-ROI additions to teach via `POST /field-types/{key}/synonyms`.\n\n")
	for _, e := range rankByCount(countsFromSynonym(a.synonymCand)) {
		b := a.synonymCand[e.Key]
		parts := strings.SplitN(e.Key, "|", 2)
		avg := 0.0
		if b.Count > 0 {
			avg = b.ScoreTotal / float64(b.Count)
		}
		fmt.Fprintf(f, "- `%s` -> `%s` (score %.2f, %d files) e.g. %s\n",
			parts[0], b.Field, avg, b.Count, strings.Join(b.Examples, ", "))
	}

	fmt.Fprintf(f, "\n## New-field candidates\n\n")
	fmt.Fprintf(f, "Unmapped by name AND by value shape. Candidates for `POST /field-types`.\n\n")
	for _, e := range rankByCount(countsFromNewField(a.newFieldCand)) {
		b := a.newFieldCand[e.Key]
		fmt.Fprintf(f, "- `%s` (%d files) samples: %s — e.g. %s\n",
			e.Key, b.Count, strings.Join(b.Samples, ", "), strings.Join(b.Examples, ", "))
	}

	fmt.Fprintf(f, "\n## Error catalogue\n\n")
	for _, e := range rankByCount(countsFromErrors(a.errorCatalogue)) {
		b := a.errorCatalogue[e.Key]
		fmt.Fprintf(f, "- %s (%d files) e.g. %s\n", e.Key, b.Count, strings.Join(b.Examples, ", "))
	}

	return nil
}
