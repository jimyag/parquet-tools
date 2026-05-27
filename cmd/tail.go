package cmd

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/jimyag/log"
	"github.com/spf13/cobra"

	"github.com/jimyag/parquet-tools/internal/dumper"
)

var tailCmd = &cobra.Command{
	Use:   "tail",
	Short: "print the last N records from a file",
	Run:   tailRun,
}

var (
	tailCount              int64
	tailConvertInt96AsTime bool
	tailFormat             string
	tailColumns            string
)

func init() {
	tailCmd.PersistentFlags().Int64VarP(&tailCount, "count", "n", 10, "number of rows to print per file (0 = all)")
	tailCmd.PersistentFlags().BoolVar(&tailConvertInt96AsTime, "convert", false, "convert int96 as time, false prints as int96")
	tailCmd.PersistentFlags().StringVarP(&tailFormat, "format", "f", "jsonl", "output format: jsonl, csv, json")
	tailCmd.PersistentFlags().StringVar(&tailColumns, "columns", "", "comma-separated column names to include (default: all)")
	rootCmd.AddCommand(tailCmd)
}

func tailRun(cmd *cobra.Command, args []string) {
	if tailFormat != "jsonl" && tailFormat != "csv" && tailFormat != "json" {
		log.Error().Msgf("unknown format %q: must be jsonl, csv, or json", tailFormat)
		return
	}

	rdrs, err := getReaders(args)
	if err != nil {
		log.Error(err).Msg("error getting readers")
		return
	}

	colFilter := parseColFilter(tailColumns)

	var csvW *csv.Writer
	if tailFormat == "csv" {
		csvW = csv.NewWriter(os.Stdout)
		defer csvW.Flush()
	}

	jsonFirst := true
	if tailFormat == "json" {
		fmt.Print("[")
	}

	csvHeaderWritten := false
	for _, rdr := range rdrs {
		totalRows := rdr.NumRows()

		n := tailCount
		if n == 0 {
			n = totalRows
		}
		skipRows := totalRows - n
		if skipRows < 0 {
			skipRows = 0
		}

		sc := rdr.MetaData().Schema
		activeIdx, activeFields := resolveColumns(sc.NumColumns(), func(c int) string {
			return sc.Column(c).Path()
		}, colFilter)

		if tailFormat == "csv" && !csvHeaderWritten {
			if err := csvW.Write(activeFields); err != nil {
				log.Error(err).Msg("error writing CSV header")
				return
			}
			csvHeaderWritten = true
		}

		// Find which row group to start from and how many rows to skip inside it.
		var startRG int
		var skipInStartRG int64
		var cumRows int64
		for r := 0; r < rdr.NumRowGroups(); r++ {
			rgRows := rdr.RowGroup(r).NumRows()
			if cumRows+rgRows > skipRows {
				startRG = r
				skipInStartRG = skipRows - cumRows
				break
			}
			cumRows += rgRows
			startRG = r + 1
		}

		for r := startRG; r < rdr.NumRowGroups(); r++ {
			rgr := rdr.RowGroup(r)
			scanners := make([]*dumper.Dumper, len(activeIdx))
			for i, colIdx := range activeIdx {
				col, err := rgr.Column(colIdx)
				if err != nil {
					log.Error(err).Int("column", colIdx).Msg("error getting column")
					return
				}
				scanners[i] = dumper.NewDumper(col, tailConvertInt96AsTime)
			}

			// Skip rows within the first selected row group.
			if r == startRG && skipInStartRG > 0 {
				for i := int64(0); i < skipInStartRG; i++ {
					vals, hasData := collectRow(scanners)
					_ = vals
					if !hasData {
						break
					}
				}
			}

			for {
				vals, hasData := collectRow(scanners)
				if !hasData {
					break
				}
				if err := printRow(tailFormat, csvW, activeFields, vals, scanners, &jsonFirst); err != nil {
					log.Error(err).Msg("error writing row")
					return
				}
			}
		}
	}

	if tailFormat == "json" {
		if !jsonFirst {
			fmt.Print("\n")
		}
		fmt.Println("]")
	}
}
