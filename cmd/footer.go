package cmd

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/spf13/cobra"
)

// metaCmd represents the meta command
var footerCmd = &cobra.Command{
	Use:   "footer",
	Short: "print the Parquet file footer in json format",
	Run:   footer,
}

func init() {
	rootCmd.AddCommand(footerCmd)
}

func footer(cmd *cobra.Command, args []string) {
	rdr := getReader()
	fileMetadata := rdr.MetaData()
	m, err := json.MarshalIndent(fileMetadata, "", "  ")
	if err != nil {
		log.Panicln(err)
	}
	fmt.Println(string(m))
}
