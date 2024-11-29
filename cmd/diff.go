package cmd

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"

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
		log.Error().Msg("diff requires two parquet files")
		return
	}
	rdrs, err := getReaders(args)
	if err != nil {
		log.Error(err).Msg("error getting readers")
		return
	}
	rdr1 := rdrs[0]
	rdr2 := rdrs[1]
	schema1 := rdr1.MetaData().Schema.Root()
	schema2 := rdr2.MetaData().Schema.Root()
	var buf1 bytes.Buffer
	var buf2 bytes.Buffer
	schema.PrintSchema(schema1, &buf1, 2)
	schema.PrintSchema(schema2, &buf2, 2)
	// 如果系统有 diff 命令，则使用系统 diff 命令
	// 将结果写到临时文件中，然后使用 diff 命令比较
	if _, err := exec.LookPath("diff"); err == nil {
		tmp1, err := os.CreateTemp("", "parquet-diff-1.txt")
		if err != nil {
			log.Error(err).Msg("error creating temp file")
			return
		}
		defer os.Remove(tmp1.Name())
		if _, err = tmp1.Write(buf1.Bytes()); err != nil {
			log.Error(err).Msg("error writing to temp file")
			return
		}
		tmp2, err := os.CreateTemp("", "parquet-diff-2.txt")
		if err != nil {
			log.Error(err).Msg("error creating temp file")
			return
		}
		defer os.Remove(tmp2.Name())
		if _, err = tmp2.Write(buf2.Bytes()); err != nil {
			log.Error(err).Msg("error writing to temp file")
			return
		}
		cmd := exec.Command("diff", "-u", tmp1.Name(), tmp2.Name())
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.Run()
		return
	}
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(buf1.String(), buf2.String(), true)
	fmt.Println(dmp.DiffPrettyText(diffs))
}
