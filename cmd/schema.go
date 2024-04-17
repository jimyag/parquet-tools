package cmd

import (
	"os"

	"github.com/apache/arrow/go/v16/parquet/schema"
	"github.com/spf13/cobra"
)

var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "print the Avro schema for a file",
	Run:   catRun,
}

func init() {
	rootCmd.AddCommand(catCmd)
}

func schemaRun(cmd *cobra.Command, args []string) {
	rdr := getReader()
	schema.PrintSchema(rdr.MetaData().Schema.Root(), os.Stdout, 2)
}
