/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/apache/arrow/go/v17/parquet/metadata"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/jimyag/log"
	"github.com/spf13/cobra"
)

// metaCmd represents the meta command
var metaCmd = &cobra.Command{
	Use:   "meta",
	Short: "print a Parquet file's metadata",
	Run:   meta,
}

func init() {
	rootCmd.AddCommand(metaCmd)
}

func meta(cmd *cobra.Command, args []string) {
	rdr := getReader()
	fileMetadata := rdr.MetaData()
	t := table.NewWriter()
	t.Style().Options.DrawBorder = true
	t.Style().Options.SeparateRows = true
	tTemp := table.Table{}
	tTemp.Render()
	t.SetColumnConfigs([]table.ColumnConfig{
		{Name: "key", WidthMax: 0, Align: text.AlignCenter},
		{Name: "value", WidthMax: 40, Align: text.AlignCenter},
	})
	t.AppendHeader(table.Row{"key", "value"}, table.RowConfig{AutoMergeAlign: text.AlignCenter})

	t.AppendRow(table.Row{"filename", filename})
	t.AppendRow(table.Row{"version", fileMetadata.Version()})
	t.AppendRow(table.Row{"created by", fileMetadata.GetCreatedBy()})
	t.AppendRow(table.Row{"num rows", rdr.NumRows()})
	kvMeta := fileMetadata.KeyValueMetadata()
	if kvMeta != nil {
		if kvMeta.Len() > 0 {
			t.AppendRow(table.Row{"key value file metadata", kvMeta.Len()})
		}
		keys := kvMeta.Keys()
		values := kvMeta.Values()
		for i := 0; i < kvMeta.Len(); i++ {
			t.AppendRow(table.Row{keys[i], values[i]})
		}
		t.AppendRow(table.Row{"number of row groups", rdr.NumRowGroups()})
		t.AppendRow(table.Row{"number of real columns", fileMetadata.Schema.Root().NumFields()})
		t.AppendRow(table.Row{"number of columns", fileMetadata.Schema.NumColumns()})
	}
	fmt.Println(t.Render())
	fmt.Println()
	for r := 0; r < rdr.NumRowGroups(); r++ {
		fmt.Println("--- row group: ", r, " begin ---")
		t := table.NewWriter()
		t.Style().Options.DrawBorder = true
		t.Style().Options.SeparateRows = true
		tTemp := table.Table{}
		tTemp.Render()

		rgr := rdr.RowGroup(r)
		rowGroupMeta := rgr.MetaData()
		t.AppendRow(table.Row{"total bytes", rowGroupMeta.TotalByteSize()})
		t.AppendRow(table.Row{"number of rows", rgr.NumRows()})
		fmt.Println(t.Render())

		newT := table.NewWriter()
		newT.Style().Options.DrawBorder = true
		newT.Style().Options.SeparateRows = true
		newtTemp := table.Table{}
		newtTemp.Render()
		newT.SetColumnConfigs([]table.ColumnConfig{
			{Name: "column", WidthMax: 10, Align: text.AlignCenter},
			{Name: "counts", WidthMax: 5, Align: text.AlignCenter},
			{Name: "min", WidthMax: 10, Align: text.AlignCenter},
			{Name: "max", WidthMax: 10, Align: text.AlignCenter},
			{Name: "nulls", WidthMax: 5, Align: text.AlignCenter},
			{Name: "distinct", WidthMax: 5, Align: text.AlignCenter},
			{Name: "compression", WidthMax: 5, Align: text.AlignCenter},
			{Name: "encodings", WidthMax: 5, Align: text.AlignCenter},
			{Name: "uncompressed size", WidthMax: 5, Align: text.AlignCenter},
			{Name: "compressed size", WidthMax: 5, Align: text.AlignCenter},
		})
		newT.AppendHeader(table.Row{"column", "counts", "min", "max", "nulls", "distinct", "compression", "encodings", "uncompressed", "compressed"})
		for c := range fileMetadata.Schema.NumColumns() {
			row := table.Row{}
			chunkMeta, err := rowGroupMeta.ColumnChunk(c)
			descRecord := fileMetadata.Schema.Column(c)
			row = append(row, descRecord.Name())
			if err != nil {
				log.Panic(err).Msg("error getting column chunk metadata")
			}
			if set, _ := chunkMeta.StatsSet(); set {
				stats, err := chunkMeta.Statistics()
				if err != nil {
					log.Panic(err).Msg("error getting column chunk statistics")
				}
				row = append(row, fmt.Sprint(chunkMeta.NumValues()))
				if stats.HasMinMax() {
					minV := metadata.GetStatValue(stats.Type(), stats.EncodeMin())
					if v, ok := minV.([]byte); ok {
						row = append(row, fmt.Sprint(string(v)))
					} else {
						row = append(row, fmt.Sprint(minV))
					}

					maxV := metadata.GetStatValue(stats.Type(), stats.EncodeMax())
					if v, ok := maxV.([]byte); ok {
						row = append(row, fmt.Sprint(string(v)))
					} else {
						row = append(row, fmt.Sprint(maxV))
					}
				} else {
					row = append(row, "-", "-")
				}
				if stats.HasNullCount() {
					row = append(row, fmt.Sprint(stats.NullCount()))
				} else {
					row = append(row, "-")
				}
				if stats.HasDistinctCount() {
					row = append(row, fmt.Sprint(stats.DistinctCount()))
				} else {
					row = append(row, "-")
				}
			} else {
				row = append(row, fmt.Sprint(chunkMeta.NumValues()), "-", "-", "-", "-")
			}
			row = append(row, fmt.Sprint(chunkMeta.Compression()))
			encodings := ""
			for _, enc := range chunkMeta.Encodings() {
				encodings += fmt.Sprint(enc) + " "
			}
			row = append(row, encodings)
			row = append(row, fmt.Sprint(chunkMeta.TotalUncompressedSize()))
			row = append(row, fmt.Sprint(chunkMeta.TotalCompressedSize()))
			newT.AppendRow(row)
		}
		fmt.Println(newT.Render())
		fmt.Println("--- row group: ", r, " end ---")
		fmt.Println()
	}

	fmt.Println(fileMetadata.Schema.String())
}
