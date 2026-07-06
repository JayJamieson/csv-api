package sniff

import (
	"bytes"
	"fmt"
	"io"
	"unicode/utf8"

	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/transform"
)

// Encoding identifies the detected source encoding of an input file.
type Encoding string

const (
	EncodingUTF8    Encoding = "utf-8"
	EncodingUTF8BOM Encoding = "utf-8-bom"
	EncodingUTF16LE Encoding = "utf-16le"
	EncodingUTF16BE Encoding = "utf-16be"
	EncodingCP1252  Encoding = "windows-1252"
)

// NormalizeResult describes what the normalization pass did to the input.
type NormalizeResult struct {
	SourceEncoding Encoding `json:"source_encoding"`
	BOMStripped    bool     `json:"bom_stripped"`
	NullsStripped  int      `json:"nulls_stripped"`
	CRLFNormalized bool     `json:"crlf_normalized"`
	BytesIn        int64    `json:"bytes_in"`
	BytesOut       int64    `json:"bytes_out"`
}

// DetectEncoding inspects the head of the input and decides how to decode it.
//
// Strategy, in order of reliability:
//  1. BOM sniffing — unambiguous when present.
//  2. UTF-16 heuristic — alternating NUL bytes in ASCII-heavy text. Excel's
//     "Unicode Text" export produces UTF-16LE without a BOM often enough that
//     this matters for supplier files.
//  3. UTF-8 validation over the sample — if it validates, take it. UTF-8 is
//     self-synchronizing, so random CP1252 bytes very rarely validate.
//  4. Fallback to Windows-1252 — the superset of Latin-1 that legacy Excel on
//     Windows actually writes. Every byte sequence is valid CP1252, so this
//     never fails; it is the correct "last resort" for AU/NZ supplier exports.
func DetectEncoding(sample []byte) Encoding {
	if bytes.HasPrefix(sample, []byte{0xEF, 0xBB, 0xBF}) {
		return EncodingUTF8BOM
	}
	if bytes.HasPrefix(sample, []byte{0xFF, 0xFE}) {
		return EncodingUTF16LE
	}
	if bytes.HasPrefix(sample, []byte{0xFE, 0xFF}) {
		return EncodingUTF16BE
	}

	if enc, ok := detectUTF16NoBOM(sample); ok {
		return enc
	}

	if utf8.Valid(sample) {
		return EncodingUTF8
	}

	return EncodingCP1252
}

// detectUTF16NoBOM looks for the NUL-interleave pattern of BOM-less UTF-16
// carrying mostly-ASCII text (which is what a CSV is). We require a strong
// signal: >40% of bytes NUL and a clear even/odd skew.
func detectUTF16NoBOM(sample []byte) (Encoding, bool) {
	if len(sample) < 16 {
		return "", false
	}
	var evenNul, oddNul int
	for i, b := range sample {
		if b == 0x00 {
			if i%2 == 0 {
				evenNul++
			} else {
				oddNul++
			}
		}
	}
	total := evenNul + oddNul
	if total*10 < len(sample)*4 { // fewer than 40% NULs: not UTF-16 text
		return "", false
	}
	if oddNul > evenNul*4 {
		return EncodingUTF16LE, true // ASCII in even positions, NUL in odd
	}
	if evenNul > oddNul*4 {
		return EncodingUTF16BE, true
	}
	return "", false
}

// Normalize reads src and writes a cleaned UTF-8 copy to dst:
//   - decodes from the detected encoding
//   - strips any BOM
//   - drops stray NUL bytes (they poison DuckDB's CSV reader)
//   - normalizes CRLF / lone CR to LF
//
// It buffers up to sniffLen bytes to detect the encoding, then streams the
// remainder through the decoder, so memory use is bounded regardless of file
// size.
func Normalize(dst io.Writer, src io.Reader) (*NormalizeResult, error) {
	const sniffLen = 64 * 1024

	head := make([]byte, sniffLen)
	n, err := io.ReadFull(src, head)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, fmt.Errorf("sniff read: %w", err)
	}
	head = head[:n]

	res := &NormalizeResult{SourceEncoding: DetectEncoding(head)}

	full := io.MultiReader(bytes.NewReader(head), src)
	res.BytesIn = int64(n) // updated below via countingReader for the tail

	var decoded io.Reader
	switch res.SourceEncoding {
	case EncodingUTF8:
		decoded = full
	case EncodingUTF8BOM:
		decoded = transform.NewReader(full, unicode.UTF8BOM.NewDecoder())
		res.BOMStripped = true
	case EncodingUTF16LE:
		decoded = transform.NewReader(full,
			unicode.UTF16(unicode.LittleEndian, unicode.UseBOM).NewDecoder())
		res.BOMStripped = bytes.HasPrefix(head, []byte{0xFF, 0xFE})
	case EncodingUTF16BE:
		decoded = transform.NewReader(full,
			unicode.UTF16(unicode.BigEndian, unicode.UseBOM).NewDecoder())
		res.BOMStripped = bytes.HasPrefix(head, []byte{0xFE, 0xFF})
	case EncodingCP1252:
		decoded = transform.NewReader(full, charmap.Windows1252.NewDecoder())
	}

	cw := &cleanWriter{w: dst, res: res}
	written, err := io.Copy(cw, decoded)
	if err != nil {
		return nil, fmt.Errorf("normalize copy: %w", err)
	}
	_ = written
	if err := cw.flushCR(); err != nil {
		return nil, err
	}
	return res, nil
}

// cleanWriter strips NULs and normalizes line endings on the fly.
// It holds back a trailing CR at each Write boundary so CRLF pairs split
// across chunks are still collapsed correctly.
type cleanWriter struct {
	w         io.Writer
	res       *NormalizeResult
	pendingCR bool
}

func (c *cleanWriter) Write(p []byte) (int, error) {
	out := make([]byte, 0, len(p)+1)

	for _, b := range p {
		if c.pendingCR {
			c.pendingCR = false
			out = append(out, '\n')
			c.res.CRLFNormalized = true
			if b == '\n' {
				continue // CRLF -> LF, swallow the LF
			}
		}
		switch b {
		case 0x00:
			c.res.NullsStripped++
		case '\r':
			c.pendingCR = true // decide when we see the next byte
		default:
			out = append(out, b)
		}
	}

	if _, err := c.w.Write(out); err != nil {
		return 0, err
	}
	c.res.BytesOut += int64(len(out))
	return len(p), nil
}

// flushCR emits a final LF if the stream ended on a bare CR.
func (c *cleanWriter) flushCR() error {
	if !c.pendingCR {
		return nil
	}
	c.pendingCR = false
	c.res.CRLFNormalized = true
	if _, err := c.w.Write([]byte{'\n'}); err != nil {
		return err
	}
	c.res.BytesOut++
	return nil
}
