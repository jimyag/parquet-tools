package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/jimyag/parquet-tools/internal/dumper"
)

var validateCmd = &cobra.Command{
	Use:   "validate",
	Short: "validate one or more Parquet files",
	Long: `Validate checks that each file can be opened and its footer decoded.
With --full the entire data payload is read and any column decode errors are reported.`,
	Run: validateRun,
}

var (
	validateFull    bool
	validateVerbose bool
)

func init() {
	validateCmd.Flags().BoolVar(&validateFull, "full", false, "read all row data (slower but catches data corruption)")
	validateCmd.Flags().BoolVarP(&validateVerbose, "verbose", "v", false, "print per-file statistics for valid files")
	rootCmd.AddCommand(validateCmd)
}

func validateRun(cmd *cobra.Command, args []string) {
	anyInvalid := false

	for _, arg := range args {
		valid, msg := validateFile(arg)
		if valid {
			if validateVerbose {
				fmt.Printf("OK  %s — %s\n", arg, msg)
			} else {
				fmt.Printf("OK  %s\n", arg)
			}
		} else {
			fmt.Fprintf(os.Stderr, "ERR %s — %s\n", arg, msg)
			anyInvalid = true
		}
	}

	if anyInvalid {
		os.Exit(1)
	}
}

// validateFile opens and, optionally, reads a parquet file.
// Returns (true, summary) on success, (false, reason) on failure.
func validateFile(path string) (bool, string) {
	rdrs, err := getReaders([]string{path})
	if err != nil {
		return false, err.Error()
	}
	if len(rdrs) == 0 {
		return false, "could not open file"
	}
	rdr := rdrs[0]

	numRows := rdr.NumRows()
	numRGs := rdr.NumRowGroups()
	numCols := rdr.MetaData().Schema.NumColumns()

	if !validateFull {
		summary := fmt.Sprintf("%d rows, %d row groups, %d columns", numRows, numRGs, numCols)
		return true, summary
	}

	// Read every value in every column of every row group.
	for r := 0; r < numRGs; r++ {
		rgr := rdr.RowGroup(r)
		for c := 0; c < numCols; c++ {
			col, err := rgr.Column(c)
			if err != nil {
				return false, fmt.Sprintf("row group %d column %d: %v", r, c, err)
			}
			sc := dumper.NewDumper(col, false)
			for {
				_, ok := sc.Next()
				if !ok {
					break
				}
			}
		}
	}

	summary := fmt.Sprintf("%d rows, %d row groups, %d columns (all data read)", numRows, numRGs, numCols)
	return true, summary
}

