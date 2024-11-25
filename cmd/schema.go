package cmd

import (
	"os"

	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/jimyag/log"
	"github.com/spf13/cobra"
)

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "print the Avro schema for a file",
	Run:   schemaRun,
}

func init() {
	rootCmd.AddCommand(schemaCmd)
}

func schemaRun(cmd *cobra.Command, args []string) {
	rdrs, err := getReaders(args)
	if err != nil {
		log.Error(err).Msg("error getting readers")
		return
	}
	for _, rdr := range rdrs {
		schema.PrintSchema(rdr.MetaData().Schema.Root(), os.Stdout, 2)
	}
}
