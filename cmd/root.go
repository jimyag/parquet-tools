/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/BurntSushi/toml"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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

var s3ConfigFile string

func init() {
	rootCmd.PersistentFlags().StringVarP(&s3ConfigFile, "s3-config", "", "", "s3 config file")
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
			cfg := s3Config{}
			if s3ConfigFile == "" {
				return nil, fmt.Errorf("s3 config file is required for s3 scheme")
			}
			if _, err := toml.DecodeFile(s3ConfigFile, &cfg); err != nil {
				return nil, err
			}
			mySession := session.Must(session.NewSession(&aws.Config{
				Credentials:      credentials.NewStaticCredentials(cfg.AccessKey, cfg.SecretKey, ""),
				Endpoint:         aws.String(cfg.EndPoint),
				Region:           aws.String(cfg.Region),
				DisableSSL:       aws.Bool(cfg.DisableSSL),
				S3ForcePathStyle: aws.Bool(cfg.ForcePathStyle),
			}))
			s3Cli := s3.New(mySession)
			s3Reader, err := reader.NewS3Reader(context.Background(), filename, s3Cli)
			if err != nil {
				return nil, err
			}
			rdr, err := file.NewParquetReader(s3Reader)
			if err != nil {
				return nil, err
			}
			readers[i] = rdr
			continue
		}
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}
	return readers, nil
}
