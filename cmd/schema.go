package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

// metaCmd represents the meta command
var schemaCmd = &cobra.Command{
	Use:   "schema",
	Short: "",
	Long:  ``,
	Run:   schema,
}

func init() {
	rootCmd.AddCommand(schemaCmd)
}

func schema(cmd *cobra.Command, args []string) {
	rdr := getReader()
	fileMetadata := rdr.MetaData()
	fmt.Println(fileMetadata.Schema.String())
}
