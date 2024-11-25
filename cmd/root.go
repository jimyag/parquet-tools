/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"

	"github.com/BurntSushi/toml"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jimyag/log"
	"github.com/spf13/cobra"

	"github.com/jimyag/parquet-tools/internal/reader"
)

var rootCmd = &cobra.Command{
	Use:   "parquet-tools",
	Short: "Utility to inspect Parquet files",
}

const (
	s3Scheme    = "s3"
	s3aScheme   = "s3a"
	localScheme = "file"
	httpScheme  = "http"
	httpsScheme = "https"

	s3ConfigFileUsage = `
# BEGIN S3 CONFIG -----
[[s3]]
endpoint = "http://127.0.0.1:9000"
region = "us-east-1"
access_key = "ak1"
secret_key = "sk1"
disable_ssl = true
force_path_style = true
scopes = ["bucket1", "bucket2"]
[[s3]]
endpoint = "http://127.0.0.1:9000"
region = "us-east-1"
access_key = "ak2"
secret_key = "sk2"
disable_ssl = true
force_path_style = true
scopes = ["bucket3", "bucket4"]
# END S3 CONFIG -----
`
)

var s3ConfigFile string = ".parquet-tools/s3.toml"

func init() {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "./"
	}
	s3ConfigFile = filepath.Join(home, s3ConfigFile)
	if _, err := os.Stat(s3ConfigFile); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(s3ConfigFile), 0700)
		os.WriteFile(s3ConfigFile, []byte(s3ConfigFileUsage), 0600)
	}
	rootCmd.PersistentFlags().StringVarP(&s3ConfigFile, "s3-config", "", s3ConfigFile, "s3 config file")
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

type Config struct {
	S3 []s3Cfg `toml:"s3" json:"s3"`
}

type s3Cfg struct {
	Region         string   `toml:"region" json:"region"`
	AccessKey      string   `toml:"access_key" json:"access_key"`
	SecretKey      string   `toml:"secret_key" json:"secret_key"`
	DisableSSL     bool     `toml:"disable_ssl" json:"disable_ssl"`
	ForcePathStyle bool     `toml:"force_path_style" json:"force_path_style"`
	EndPoint       string   `toml:"endpoint" json:"endpoint"`
	Scopes         []string `toml:"scopes" json:"scopes"`
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
			cfg := Config{}
			if _, err := toml.DecodeFile(s3ConfigFile, &cfg); err != nil {
				return nil, err
			}
			for ic, c := range cfg.S3 {
				mySession := session.Must(session.NewSession(&aws.Config{
					Credentials:      credentials.NewStaticCredentials(c.AccessKey, c.SecretKey, ""),
					Endpoint:         aws.String(c.EndPoint),
					Region:           aws.String(c.Region),
					DisableSSL:       aws.Bool(c.DisableSSL),
					S3ForcePathStyle: aws.Bool(c.ForcePathStyle),
				}))
				bucket, _, err := reader.ParsePath(filename)
				if err != nil {
					return nil, err
				}
				s3Cli := s3.New(mySession)
				_, err = reader.Stat(context.Background(), filename, s3Cli)
				if err == nil {
					s3Reader, err := reader.NewS3Reader(context.Background(), filename, s3Cli)
					if err != nil {
						return nil, err
					}
					rdr, err := file.NewParquetReader(s3Reader)
					if err != nil {
						return nil, err
					}
					readers[i] = rdr
					if c.Scopes == nil {
						c.Scopes = []string{}
					}
					if slices.Contains(c.Scopes, bucket) {
						continue
					}
					c.Scopes = append(c.Scopes, bucket)
					cfg.S3[ic] = c
					// update config file
					f, err := os.OpenFile(s3ConfigFile, os.O_WRONLY, 0600)
					if err != nil {
						log.Error().Msgf("error opening s3 config file: %s", err)
						return nil, err
					}
					if err := toml.NewEncoder(f).Encode(cfg); err != nil {
						log.Error().Msgf("error encoding s3 config file: %s", err)
						return nil, err
					}
					f.Close()
					break
				}
				for _, scope := range c.Scopes {
					if scope == bucket {
						s3Reader, err := reader.NewS3Reader(context.Background(), filename, s3Cli)
						if err != nil {
							return nil, err
						}
						rdr, err := file.NewParquetReader(s3Reader)
						if err != nil {
							return nil, err
						}
						readers[i] = rdr
						break
					}
				}
			}
			if readers[i] == nil {
				return nil, fmt.Errorf("don't have access to %s", filename)
			}
		}
	}
	return readers, nil
}
