package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/jimyag/log"
	"github.com/spf13/cobra"

	"github.com/jimyag/parquet-tools/internal/dumper"
)

var catCmd = &cobra.Command{
	Use:   "cat",
	Short: "print a Parquet file in json format",
	Run:   catRun,
}

var parseInt96AsTime bool

func init() {
	rootCmd.AddCommand(catCmd)
	catCmd.PersistentFlags().BoolVarP(&parseInt96AsTime, "parse-int96-as-timestamp", "", false, "parse int96 as time,false print as int96")
}

func catRun(cmd *cobra.Command, args []string) {
	rdr := getReader()
	dataOut := os.Stdout
	for r := 0; r < rdr.NumRowGroups(); r++ {
		rgr := rdr.RowGroup(r)
		scanners := make([]*dumper.Dumper, rdr.MetaData().Schema.NumColumns())
		fields := make([]string, rdr.MetaData().Schema.NumColumns())
		for c := range rdr.MetaData().Schema.NumColumns() {
			col, err := rgr.Column(c)
			if err != nil {
				log.Panic(err).Int("column", c).Msg("error getting column")
			}
			scanners[c] = dumper.NewDumper(col, parseInt96AsTime)
			fields[c] = col.Descriptor().Path()
		}
		var line string
		for {
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
						fmt.Fprint(dataOut, line)
					}
					data = true
					if val == nil {
						continue
					}
					if !first {
						fmt.Fprint(dataOut, ",")
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
					fmt.Fprintf(dataOut, "%q: %s", fields[idx], jsonVal)
				}
			}
			if !data {
				break
			}
			fmt.Fprint(dataOut, "}")
		}
		fmt.Fprintln(dataOut)
	}
}
