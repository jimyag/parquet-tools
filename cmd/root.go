/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	"net/url"
	"os"

	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/spf13/cobra"

	"github.com/jimyag/parquet-tools/internal/reader"
)

var rootCmd = &cobra.Command{
	Use:   "parquet-tools",
	Short: "Utility to inspect Parquet files",
	Run:   func(cmd *cobra.Command, args []string) {},
}

const (
	s3Scheme    = "s3"
	s3aScheme   = "s3a"
	localScheme = "file"
	httpScheme  = "http"
	httpsScheme = "https"

	s3ConfigFileUsage = `s3 config file format:
	----- BEGIN S3 CONFIG -----
	endpoint = "http://127.0.0.1:9000"
	region = "us-east-1"
	access_key = "ak"
	secret_key = "sk"
	disable_ssl = true
	force_path_style = true
	----- END S3 CONFIG -----

	`
)

func init() {
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

type s3Config struct {
	Region         string `toml:"region" json:"region"`
	AccessKey      string `toml:"access_key" json:"access_key"`
	SecretKey      string `toml:"secret_key" json:"secret_key"`
	DisableSSL     bool   `toml:"disable_ssl" json:"disable_ssl"`
	ForcePathStyle bool   `toml:"force_path_style" json:"force_path_style"`
	EndPoint       string `toml:"endpoint" json:"endpoint"`
}

func getReaders(filenames []string) ([]*file.Reader, error) {
	readers := make([]*file.Reader, len(filenames))
	for i, filename := range filenames {
		u, err := url.Parse(filename)
		if err != nil {
			return nil, err
		}
		if u.Scheme == "" || u.Scheme == localScheme {
			rdr, err := file.OpenParquetFile(filename, false)
			if err != nil {
				return nil, err
			}
			readers[i] = rdr
			continue
		}

		if u.Scheme == httpScheme || u.Scheme == httpsScheme {
			httpReader, err := reader.NewHttpReader(filename)
			if err != nil {
				return nil, err
			}
			rdr, err := file.NewParquetReader(httpReader)
			if err != nil {
				return nil, err
			}
			readers[i] = rdr
			continue
		}
		if u.Scheme == s3Scheme || u.Scheme == s3aScheme {
			// if configFile != "" {
			// 	cfg := s3Config{}
			// 	if _, err := toml.DecodeFile(configFile, &cfg); err != nil {
			// 		log.Panic(err).Msg("error decoding config file")
			// 	}
			// 	region = cfg.Region
			// 	access_key = cfg.AccessKey
			// 	secret_key = cfg.SecretKey
			// 	disableSSL = cfg.DisableSSL
			// 	forcePathStyle = cfg.ForcePathStyle
			// 	endpoint = cfg.EndPoint
			// }
			// if endpoint == "" {
			// 	log.Panic().Msg("end_point is required for s3 scheme")
			// }
			// mySession := session.Must(session.NewSession(&aws.Config{
			// 	Credentials:      credentials.NewStaticCredentials(access_key, secret_key, ""),
			// 	Endpoint:         aws.String(endpoint),
			// 	Region:           aws.String(region),
			// 	DisableSSL:       aws.Bool(disableSSL),
			// 	S3ForcePathStyle: aws.Bool(forcePathStyle),
			// }))
			// s3Cli := s3.New(mySession)
			// s3Reader, err := reader.NewS3Reader(context.Background(), filename, s3Cli)
			// if err != nil {
			// 	return nil, err
			// }
			// rdr, err := file.NewParquetReader(s3Reader)
			// if err != nil {
			// 	return nil, err
			// }
			// readers[i] = rdr
			// continue
		}
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	return readers, nil
}
