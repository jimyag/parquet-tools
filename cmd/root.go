/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/metadata"
	"github.com/jimyag/log"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "parquet-tools",
	Short: "A brief description of your application",
	Long:  ``,
	Run:   func(cmd *cobra.Command, args []string) {},
}
var filename string

func init() {
	rootCmd.PersistentFlags().StringVarP(&filename, "filename", "f", "", "filename")
	_ = rootCmd.MarkPersistentFlagRequired("filename")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func getReader() *file.Reader {
	rdr, err := file.OpenParquetFile(filename, false)
	if err != nil {
		log.Panic(err).Msg("error opening parquet file")
	}
	return rdr
}

func read() {
	rdr, err := file.OpenParquetFile(os.Args[1], false)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error opening parquet file: ", err)
		os.Exit(1)
	}

	fileMetadata := rdr.MetaData()

	selectedColumns := []int{}
	for i := 0; i < fileMetadata.Schema.NumColumns(); i++ {
		selectedColumns = append(selectedColumns, i)
	}

	for r := 0; r < rdr.NumRowGroups(); r++ {
		fmt.Println("--- Row Group:", r, " ---")

		rgr := rdr.RowGroup(r)
		rowGroupMeta := rgr.MetaData()
		fmt.Println("--- Total Bytes:", rowGroupMeta.TotalByteSize(), " ---")
		fmt.Println("--- Rows:", rgr.NumRows(), " ---")

		for _, c := range selectedColumns {
			chunkMeta, err := rowGroupMeta.ColumnChunk(c)
			if err != nil {
				log.Panic(err).Msg("error getting column chunk metadata")
			}

			fmt.Println("Column", c)
			if set, _ := chunkMeta.StatsSet(); set {
				stats, err := chunkMeta.Statistics()
				if err != nil {
					log.Panic(err).Msg("error getting column chunk statistics")
				}
				fmt.Printf(" Values: %d", chunkMeta.NumValues())
				if stats.HasMinMax() {
					fmt.Printf(", Min: %v, Max: %v",
						metadata.GetStatValue(stats.Type(), stats.EncodeMin()),
						metadata.GetStatValue(stats.Type(), stats.EncodeMax()))
				}
				if stats.HasNullCount() {
					fmt.Printf(", Null Values: %d", stats.NullCount())
				}
				if stats.HasDistinctCount() {
					fmt.Printf(", Distinct Values: %d", stats.DistinctCount())
				}
				fmt.Println()
			} else {
				fmt.Println(" Values:", chunkMeta.NumValues(), "Statistics Not Set")
			}

			fmt.Print(" Compression: ", chunkMeta.Compression())
			fmt.Print(", Encodings:")
			for _, enc := range chunkMeta.Encodings() {
				fmt.Print(" ", enc)
			}
			fmt.Println()

			fmt.Print(" Uncompressed Size: ", chunkMeta.TotalUncompressedSize())
			fmt.Println(", Compressed Size:", chunkMeta.TotalCompressedSize())
		}

		// fmt.Println("--- Values ---")
		// const colwidth = 18

		// scanners := make([]*Dumper, len(selectedColumns))
		// for idx, c := range selectedColumns {
		// 	col, err := rgr.Column(c)
		// 	if err != nil {
		// 		log.Panic(err).
		// 			Int("column", c).
		// 			Msg("unable to fetch column data")
		// 	}
		// 	scanners[idx] = createDumper(col)
		// 	fmt.Fprintf(os.Stdout, fmt.Sprintf("%%-%ds|", colwidth), col.Descriptor().Name())
		// }
		// fmt.Fprintln(os.Stdout)

		// var line string
		// for {
		// 	data := false
		// 	for _, s := range scanners {
		// 		if val, ok := s.Next(); ok {
		// 			if !data {
		// 				fmt.Fprint(os.Stdout, line)
		// 			}
		// 			fmt.Fprint(os.Stdout, s.FormatValue(val, colwidth), "|")
		// 			data = true
		// 		} else {
		// 			if data {
		// 				fmt.Fprintf(os.Stdout, fmt.Sprintf("%%-%ds|", colwidth), "")
		// 			} else {
		// 				line += fmt.Sprintf(fmt.Sprintf("%%-%ds|", colwidth), "")
		// 			}
		// 		}
		// 	}
		// 	if !data {
		// 		break
		// 	}
		// 	fmt.Fprintln(os.Stdout)
		// 	line = ""
		// }
		fmt.Fprintln(os.Stdout)
	}
}
