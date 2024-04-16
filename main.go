package main

import (
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/metadata"
	"github.com/apache/arrow/go/v16/parquet/schema"
)

var version = ""
var usage = `Parquet Reader (version ` + version + `)
Usage:
  parquet_reader -h | --help
  parquet_reader [--only-metadata] [--no-metadata] [--no-memory-map] [--json] [--csv] [--output=FILE]
                 [--print-key-value-metadata] [--int96-timestamp] [--columns=COLUMNS] <file>
Options:
  -h --help                     Show this screen.
  --print-key-value-metadata    Print out the key-value metadata. [default: false]
  --only-metadata               Stop after printing metadata, no values.
  --no-metadata                 Do not print metadata.
  --output=FILE                 Specify output file for data. [default: -]
  --no-memory-map               Disable memory mapping the file.
  --int96-timestamp             Parse INT96 as TIMESTAMP for legacy support.
  --json                        Format output as JSON instead of text.
  --csv                         Format output as CSV instead of text.
  --columns=COLUMNS             Specify a subset of columns to print, comma delimited indexes.`

func main() {

	rdr, err := file.OpenParquetFile(os.Args[1], false)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error opening parquet file: ", err)
		os.Exit(1)
	}

	fileMetadata := rdr.MetaData()

	fmt.Println("File name:", os.Args[1])
	fmt.Println("Version:", fileMetadata.Version())
	fmt.Println("Created By:", fileMetadata.GetCreatedBy())
	fmt.Println("Num Rows:", rdr.NumRows())

	keyvaluemeta := fileMetadata.KeyValueMetadata()
	if keyvaluemeta != nil {
		fmt.Println("Key Value File Metadata:", keyvaluemeta.Len(), "entries")
		keys := keyvaluemeta.Keys()
		values := keyvaluemeta.Values()
		for i := 0; i < keyvaluemeta.Len(); i++ {
			fmt.Printf("Key nr %d %s: %s\n", i, keys[i], values[i])
		}

		fmt.Println("Number of RowGroups:", rdr.NumRowGroups())
		fmt.Println("Number of Real Columns:", fileMetadata.Schema.Root().NumFields())
		fmt.Println("Number of Columns:", fileMetadata.Schema.NumColumns())
	}
	selectedColumns := []int{}
	for i := 0; i < fileMetadata.Schema.NumColumns(); i++ {
		selectedColumns = append(selectedColumns, i)
	}

	fmt.Println("Number of Selected Columns:", len(selectedColumns))
	for _, c := range selectedColumns {
		descr := fileMetadata.Schema.Column(c)
		fmt.Printf("Column %d: %s (%s", c, descr.Path(), descr.PhysicalType())
		if descr.ConvertedType() != schema.ConvertedTypes.None {
			fmt.Printf("/%s", descr.ConvertedType())
			if descr.ConvertedType() == schema.ConvertedTypes.Decimal {
				dec := descr.LogicalType().(*schema.DecimalLogicalType)
				fmt.Printf("(%d,%d)", dec.Precision(), dec.Scale())
			}
		}
		fmt.Print(")\n")
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
				log.Fatal(err)
			}

			fmt.Println("Column", c)
			if set, _ := chunkMeta.StatsSet(); set {
				stats, err := chunkMeta.Statistics()
				if err != nil {
					log.Fatal(err)
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

		fmt.Println("--- Values ---")
		const colwidth = 18

		scanners := make([]*Dumper, len(selectedColumns))
		for idx, c := range selectedColumns {
			col, err := rgr.Column(c)
			if err != nil {
				log.Fatalf("unable to fetch column=%d err=%s", c, err)
			}
			scanners[idx] = createDumper(col)
			fmt.Fprintf(os.Stdout, fmt.Sprintf("%%-%ds|", colwidth), col.Descriptor().Name())
		}
		fmt.Fprintln(os.Stdout)

		var line string
		for {
			data := false
			for _, s := range scanners {
				if val, ok := s.Next(); ok {
					if !data {
						fmt.Fprint(os.Stdout, line)
					}
					fmt.Fprint(os.Stdout, s.FormatValue(val, colwidth), "|")
					data = true
				} else {
					if data {
						fmt.Fprintf(os.Stdout, fmt.Sprintf("%%-%ds|", colwidth), "")
					} else {
						line += fmt.Sprintf(fmt.Sprintf("%%-%ds|", colwidth), "")
					}
				}
			}
			if !data {
				break
			}
			fmt.Fprintln(os.Stdout)
			line = ""
		}
		fmt.Fprintln(os.Stdout)
	}
}
