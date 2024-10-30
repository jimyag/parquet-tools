package cmd

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/jimyag/log"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/spf13/cobra"
)

var diffCmd = &cobra.Command{
	Use:   "diff",
	Short: "print the schema diff between two parquet files",
	Run:   diffRun,
}

func init() {
	rootCmd.AddCommand(diffCmd)
}

func diffRun(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		log.Panic().Msg("diff requires two parquet files")
	}
	rdrs, err := getReaders(args)
	if err != nil {
		log.Panic(err).Msg("error getting readers")
	}
	rdr1 := rdrs[0]
	rdr2 := rdrs[1]
	schema1 := rdr1.MetaData().Schema.Root()
	schema2 := rdr2.MetaData().Schema.Root()
	var buf1 bytes.Buffer
	var buf2 bytes.Buffer
	schema.PrintSchema(schema1, &buf1, 2)
	schema.PrintSchema(schema2, &buf2, 2)
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(buf1.String(), buf2.String(), true)
	fmt.Println(dmp.DiffPrettyText(diffs))
}
