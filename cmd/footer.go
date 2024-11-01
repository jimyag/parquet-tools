package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/jimyag/log"
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
	rdrs, err := getReaders(args)
	if err != nil {
		log.Panic(err).Msg("error getting readers")
	}
	for _, rdr := range rdrs {
		fileMetadata := rdr.MetaData()
		m, err := json.MarshalIndent(fileMetadata, "", "  ")
		if err != nil {
			log.Panic(err).Msg("error marshalling file metadata")
		}
		fmt.Println(string(m))
	}
}
