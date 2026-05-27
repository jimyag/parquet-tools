package cmd

import (
	"fmt"

	"github.com/jimyag/log"
	"github.com/spf13/cobra"
)

var rowcountCmd = &cobra.Command{
	Use:   "rowcount",
	Short: "print the number of rows in a Parquet file",
	Run:   rowcountRun,
}

func init() {
	rootCmd.AddCommand(rowcountCmd)
}

func rowcountRun(cmd *cobra.Command, args []string) {
	rdrs, err := getReaders(args)
	if err != nil {
		log.Error(err).Msg("error getting readers")
		return
	}
	for i, rdr := range rdrs {
		if len(rdrs) == 1 {
			fmt.Println(rdr.NumRows())
		} else {
			fmt.Printf("%s\t%d\n", args[i], rdr.NumRows())
		}
	}
}
