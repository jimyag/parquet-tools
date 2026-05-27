package cmd

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/jimyag/log"
	"github.com/spf13/cobra"

	"github.com/jimyag/parquet-tools/internal/dumper"
)

var catCmd = &cobra.Command{
	Use:   "cat",
	Short: "print the first N records from a file",
	Run:   catRun,
}

var (
	convertInt96AsTime bool
	count              int64
	catFormat          string
	catColumns         string
)

func init() {
	catCmd.PersistentFlags().BoolVar(&convertInt96AsTime, "convert", false, "convert int96 as time, false prints as int96")
	catCmd.PersistentFlags().Int64VarP(&count, "count", "n", 0, "number of rows to print per file (0 = all)")
	catCmd.PersistentFlags().StringVarP(&catFormat, "format", "f", "jsonl", "output format: jsonl, csv, json")
	catCmd.PersistentFlags().StringVar(&catColumns, "columns", "", "comma-separated column names to include (default: all)")
	rootCmd.AddCommand(catCmd)
}

func catRun(cmd *cobra.Command, args []string) {
	if catFormat != "jsonl" && catFormat != "csv" && catFormat != "json" {
		log.Error().Msgf("unknown format %q: must be jsonl, csv, or json", catFormat)
		return
	}

	rdrs, err := getReaders(args)
	if err != nil {
		log.Error(err).Msg("error getting readers")
		return
	}

	colFilter := parseColFilter(catColumns)

	var csvW *csv.Writer
	if catFormat == "csv" {
		csvW = csv.NewWriter(os.Stdout)
		defer csvW.Flush()
	}

	jsonFirst := true
	if catFormat == "json" {
		fmt.Print("[")
	}

	csvHeaderWritten := false
	for _, rdr := range rdrs {
		// Per-file row limit: user-supplied count, or all rows in this file.
		fileCount := count
		if fileCount == 0 {
			fileCount = rdr.MetaData().NumRows + 1
		}

		sc := rdr.MetaData().Schema
		activeIdx, activeFields := resolveColumns(sc.NumColumns(), func(c int) string {
			return sc.Column(c).Path()
		}, colFilter)

		// Write CSV header once across all files (assumes consistent schema).
		if catFormat == "csv" && !csvHeaderWritten {
			if err := csvW.Write(activeFields); err != nil {
				log.Error(err).Msg("error writing CSV header")
				return
			}
			csvHeaderWritten = true
		}

		var printNum int64
	rowGroupLoop:
		for r := 0; r < rdr.NumRowGroups(); r++ {
			rgr := rdr.RowGroup(r)
			scanners := make([]*dumper.Dumper, len(activeIdx))
			for i, colIdx := range activeIdx {
				col, err := rgr.Column(colIdx)
				if err != nil {
					log.Error(err).Int("column", colIdx).Msg("error getting column")
					return
				}
				scanners[i] = dumper.NewDumper(col, convertInt96AsTime)
			}

			for {
				if printNum >= fileCount {
					break rowGroupLoop
				}
				vals, hasData := collectRow(scanners)
				if !hasData {
					break
				}
				if err := printRow(catFormat, csvW, activeFields, vals, scanners, &jsonFirst); err != nil {
					log.Error(err).Msg("error writing row")
					return
				}
				printNum++
			}
		}
	}

	if catFormat == "json" {
		if !jsonFirst {
			fmt.Print("\n")
		}
		fmt.Println("]")
	}
}
