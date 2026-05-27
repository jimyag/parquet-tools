package cmd

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jimyag/parquet-tools/internal/dumper"
)

// renderJSONObject renders a row as a compact JSON object.
// Null fields are rendered as JSON null and are never omitted.
func renderJSONObject(fields []string, vals []interface{}, scanners []*dumper.Dumper) string {
	var sb strings.Builder
	sb.WriteByte('{')
	for i, field := range fields {
		if i > 0 {
			sb.WriteByte(',')
		}
		key, _ := json.Marshal(field)
		sb.Write(key)
		sb.WriteByte(':')
		v := vals[i]
		if v == nil {
			sb.WriteString("null")
		} else {
			switch v.(type) {
			case bool, int32, int64, float32, float64:
			default:
				v = scanners[i].FormatValue(v, 0)
			}
			jsonVal, _ := json.Marshal(v)
			sb.Write(jsonVal)
		}
	}
	sb.WriteByte('}')
	return sb.String()
}

// csvString converts a parquet value to its string form for CSV output.
// Null values become empty strings.
func csvString(s *dumper.Dumper, v interface{}) string {
	if v == nil {
		return ""
	}
	switch v.(type) {
	case bool, int32, int64, float32, float64:
		return fmt.Sprintf("%v", v)
	default:
		return s.FormatValue(v, 0)
	}
}

// parseColFilter parses a comma-separated column name list into a lookup set.
// Returns nil when the input is empty, meaning "include all columns".
func parseColFilter(cols string) map[string]bool {
	if cols == "" {
		return nil
	}
	filter := make(map[string]bool)
	for _, c := range strings.Split(cols, ",") {
		if s := strings.TrimSpace(c); s != "" {
			filter[s] = true
		}
	}
	if len(filter) == 0 {
		return nil
	}
	return filter
}

// resolveColumns returns the column indices and their paths to include,
// filtered by nameSet (nil = all columns).
func resolveColumns(numCols int, pathFn func(int) string, nameSet map[string]bool) ([]int, []string) {
	var indices []int
	var paths []string
	for c := range numCols {
		path := pathFn(c)
		if nameSet == nil || nameSet[path] {
			indices = append(indices, c)
			paths = append(paths, path)
		}
	}
	return indices, paths
}

// collectRow reads one value per scanner. Returns (vals, hasData).
// hasData is false only when all scanners are exhausted (end of row group).
func collectRow(scanners []*dumper.Dumper) ([]interface{}, bool) {
	vals := make([]interface{}, len(scanners))
	hasData := false
	for i, s := range scanners {
		v, ok := s.Next()
		if ok {
			hasData = true
		}
		vals[i] = v // nil means SQL-NULL
	}
	return vals, hasData
}

// printRow writes one row to the chosen output format.
// jsonFirst tracks whether this is the first row in a json array (for comma insertion).
func printRow(format string, csvW interface{ Write([]string) error }, fields []string, vals []interface{}, scanners []*dumper.Dumper, jsonFirst *bool) error {
	switch format {
	case "csv":
		record := make([]string, len(fields))
		for i := range fields {
			record[i] = csvString(scanners[i], vals[i])
		}
		return csvW.Write(record)
	case "json":
		row := renderJSONObject(fields, vals, scanners)
		if *jsonFirst {
			fmt.Printf("\n%s", row)
			*jsonFirst = false
		} else {
			fmt.Printf(",\n%s", row)
		}
	default: // jsonl
		fmt.Println(renderJSONObject(fields, vals, scanners))
	}
	return nil
}
