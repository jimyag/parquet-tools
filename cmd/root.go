/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/metadata"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jimyag/log"
	"github.com/spf13/cobra"

	"github.com/jimyag/parquet-tools/internal"
)

var rootCmd = &cobra.Command{
	Use:   "parquet-tools",
	Short: "Utility to inspect Parquet files",
	Run:   func(cmd *cobra.Command, args []string) {},
}

var (
	filename       string
	region         string
	access_key     string
	secret_key     string
	end_point      string
	disableSSL     bool
	forcePathStyle bool
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&filename, "filename", "f", "", "filename")
	rootCmd.PersistentFlags().StringVarP(&region, "region", "r", "", "s3 region")
	rootCmd.PersistentFlags().StringVarP(&access_key, "ak", "", "", "s3 access_key")
	rootCmd.PersistentFlags().StringVarP(&secret_key, "sk", "", "", "s3 secret_key")
	rootCmd.PersistentFlags().StringVarP(&end_point, "ep", "", "", "s3 end_point")
	rootCmd.PersistentFlags().BoolVarP(&disableSSL, "ds", "", false, "s3 disable_ssl")
	rootCmd.PersistentFlags().BoolVarP(&forcePathStyle, "fp", "", true, "s3 force_path_style")
	_ = rootCmd.MarkPersistentFlagRequired("filename")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

const (
	s3Scheme    = "s3"
	s3aScheme   = "s3a"
	localScheme = "file"
)

func getReader() *file.Reader {
	u, err := url.Parse(filename)
	if err != nil {
		log.Panic(err).
			Str("filename", filename).
			Msg("error parsing filename")
	}
	if u.Scheme == "" || u.Scheme == localScheme {
		rdr, err := file.OpenParquetFile(filename, false)
		if err != nil {
			log.Panic(err).Msg("error opening parquet file")
		}
		return rdr
	}
	if u.Scheme == s3Scheme || u.Scheme == s3aScheme {
		if end_point == "" {
			log.Panic().Msg("end_point is required for s3 scheme")
		}
		mySession := session.Must(session.NewSession(&aws.Config{
			Credentials:      credentials.NewStaticCredentials(access_key, secret_key, ""),
			Endpoint:         aws.String(end_point),
			Region:           aws.String(region),
			DisableSSL:       aws.Bool(disableSSL),
			S3ForcePathStyle: aws.Bool(forcePathStyle),
		}))
		s3Cli := s3.New(mySession)
		s3Reader, err := internal.NewS3Reader(context.Background(), filename, s3Cli)
		if err != nil {
			log.Panic(err).Msg("error creating s3 reader")
		}
		rdr, err := file.NewParquetReader(s3Reader)
		if err != nil {
			log.Panic(err).Msg("error creating parquet reader")
		}
		return rdr
	}
	log.Panic().Msg("unsupported scheme")
	return nil
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
