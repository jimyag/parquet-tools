package cmd

import (
	"encoding/json"
	"fmt"

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
)

func init() {
	catCmd.PersistentFlags().BoolVarP(&convertInt96AsTime, "convert", "", false, "convert int96 as time,false print as int96")
	catCmd.PersistentFlags().Int64VarP(&count, "count", "n", 0, "print count rows")
	rootCmd.AddCommand(catCmd)
}

func catRun(cmd *cobra.Command, args []string) {
	rdr := getReader()
	if count == 0 {
		count = rdr.MetaData().NumRows + 1
	}
	var printNum int64
	for r := 0; r < rdr.NumRowGroups(); r++ {
		rgr := rdr.RowGroup(r)
		scanners := make([]*dumper.Dumper, rdr.MetaData().Schema.NumColumns())
		fields := make([]string, rdr.MetaData().Schema.NumColumns())
		for c := range rdr.MetaData().Schema.NumColumns() {
			col, err := rgr.Column(c)
			if err != nil {
				log.Panic(err).Int("column", c).Msg("error getting column")
			}
			scanners[c] = dumper.NewDumper(col, convertInt96AsTime)
			fields[c] = col.Descriptor().Path()
		}
		var line string
		for {
			// printNum is used to limit the number of rows
			if printNum >= count {
				return
			}
			if line == "" {
				line = "{"
			} else {
				line = "\n{"
			}

			data := false
			first := true
			for idx, s := range scanners {
				if val, ok := s.Next(); ok {
					if !data {
						fmt.Print(line)
					}
					data = true
					if val == nil {
						continue
					}
					if !first {
						fmt.Print(",")
					}
					first = false
					switch val.(type) {
					case bool, int32, int64, float32, float64:
					default:
						val = s.FormatValue(val, 0)
					}
					jsonVal, err := json.Marshal(val)
					if err != nil {
						log.Panic(err).
							Str("val", fmt.Sprintf("%+v", val)).
							Msg("error marshalling json")
					}
					fmt.Printf("%q: %s", fields[idx], jsonVal)
				}
			}
			if !data {
				break
			}
			fmt.Print("}")
			printNum++
		}
		fmt.Println()
	}
}
