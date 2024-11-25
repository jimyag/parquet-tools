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
		log.Error().Msgf("error getting readers: %s", err)
		return
	}
	for _, rdr := range rdrs {
		fileMetadata := rdr.MetaData()
		m, err := json.MarshalIndent(fileMetadata, "", "  ")
		if err != nil {
			log.Error().Msgf("error marshalling file metadata: %s", err)
			return
		}
		fmt.Println(string(m))
	}
}
