package main

import (
	"log"

	"github.com/jimyag/go-parquet/parquet"
	"github.com/jimyag/go-parquet/source/local"
	"github.com/jimyag/go-parquet/writer"
)

type Dummy struct {
	Dummy int32 `parquet:"name=dummy, type=INT32"`
}

func main() {
	fw, err := local.NewLocalFileWriter("empty.parquet")
	if err != nil {
		log.Println("Can't create file", err)
		return
	}
	pw, err := writer.NewParquetWriter(fw, new(Dummy), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
	}
	fw.Close()
}
