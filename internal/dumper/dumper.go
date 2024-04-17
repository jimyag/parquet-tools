package dumper

import (
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/apache/arrow/go/v16/parquet"
	"github.com/apache/arrow/go/v16/parquet/file"
	"github.com/apache/arrow/go/v16/parquet/schema"
)

const defaultBatchSize = 128

type Dumper struct {
	reader         file.ColumnChunkReader
	batchSize      int64
	valueOffset    int
	valuesBuffered int

	levelOffset    int64
	levelsBuffered int64
	defLevels      []int16
	repLevels      []int16

	valueBuffer      interface{}
	parseInt96AsTime bool
}

func NewDumper(reader file.ColumnChunkReader, parseInt96AsTime bool) *Dumper {
	batchSize := defaultBatchSize

	var valueBuffer interface{}
	switch reader.(type) {
	case *file.BooleanColumnChunkReader:
		valueBuffer = make([]bool, batchSize)
	case *file.Int32ColumnChunkReader:
		valueBuffer = make([]int32, batchSize)
	case *file.Int64ColumnChunkReader:
		valueBuffer = make([]int64, batchSize)
	case *file.Float32ColumnChunkReader:
		valueBuffer = make([]float32, batchSize)
	case *file.Float64ColumnChunkReader:
		valueBuffer = make([]float64, batchSize)
	case *file.Int96ColumnChunkReader:
		valueBuffer = make([]parquet.Int96, batchSize)
	case *file.ByteArrayColumnChunkReader:
		valueBuffer = make([]parquet.ByteArray, batchSize)
	case *file.FixedLenByteArrayColumnChunkReader:
		valueBuffer = make([]parquet.FixedLenByteArray, batchSize)
	}

	return &Dumper{
		reader:           reader,
		batchSize:        int64(batchSize),
		defLevels:        make([]int16, batchSize),
		repLevels:        make([]int16, batchSize),
		valueBuffer:      valueBuffer,
		parseInt96AsTime: parseInt96AsTime,
	}
}

func (dump *Dumper) readNextBatch() {
	switch reader := dump.reader.(type) {
	case *file.BooleanColumnChunkReader:
		values := dump.valueBuffer.([]bool)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int32ColumnChunkReader:
		values := dump.valueBuffer.([]int32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int64ColumnChunkReader:
		values := dump.valueBuffer.([]int64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float32ColumnChunkReader:
		values := dump.valueBuffer.([]float32)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Float64ColumnChunkReader:
		values := dump.valueBuffer.([]float64)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.Int96ColumnChunkReader:
		values := dump.valueBuffer.([]parquet.Int96)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.ByteArrayColumnChunkReader:
		values := dump.valueBuffer.([]parquet.ByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	case *file.FixedLenByteArrayColumnChunkReader:
		values := dump.valueBuffer.([]parquet.FixedLenByteArray)
		dump.levelsBuffered, dump.valuesBuffered, _ = reader.ReadBatch(dump.batchSize, values, dump.defLevels, dump.repLevels)
	}

	dump.valueOffset = 0
	dump.levelOffset = 0
}

func (dump *Dumper) hasNext() bool {
	return dump.levelOffset < dump.levelsBuffered || dump.reader.HasNext()
}

const microSecondsPerDay = 24 * 3600e6

func (dump *Dumper) FormatValue(val interface{}, width int) string {
	fmtStr := fmt.Sprintf("-%d", width)
	switch val := val.(type) {
	case nil:
		return fmt.Sprintf("%"+fmtStr+"s", "NULL")
	case bool:
		return fmt.Sprintf("%"+fmtStr+"t", val)
	case int32:
		return fmt.Sprintf("%"+fmtStr+"d", val)
	case int64:
		return fmt.Sprintf("%"+fmtStr+"d", val)
	case float32:
		return fmt.Sprintf("%"+fmtStr+"f", val)
	case float64:
		return fmt.Sprintf("%"+fmtStr+"f", val)
	case parquet.Int96:
		if dump.parseInt96AsTime {
			return fmt.Sprintf("%"+fmtStr+"s", val.String())
		} else {
			return fmt.Sprintf("%"+fmtStr+"s",
				fmt.Sprintf("%d%d%d",
					binary.LittleEndian.Uint32(val[:4]),
					binary.LittleEndian.Uint32(val[4:]),
					binary.LittleEndian.Uint32(val[8:])))
		}
	case parquet.ByteArray:
		if dump.reader.Descriptor().ConvertedType() == schema.ConvertedTypes.UTF8 {
			return fmt.Sprintf("%"+fmtStr+"s", string(val))
		}
		return fmt.Sprintf("% "+fmtStr+"X", val)
	case parquet.FixedLenByteArray:
		return fmt.Sprintf("% "+fmtStr+"X", val)
	default:
		return fmt.Sprintf("%"+fmtStr+"s", fmt.Sprintf("%v", val))
	}
}

func (dump *Dumper) Next() (interface{}, bool) {
	if dump.levelOffset == dump.levelsBuffered {
		if !dump.hasNext() {
			return nil, false
		}
		dump.readNextBatch()
		if dump.levelsBuffered == 0 {
			return nil, false
		}
	}

	defLevel := dump.defLevels[int(dump.levelOffset)]
	dump.levelOffset++

	if defLevel < dump.reader.Descriptor().MaxDefinitionLevel() {
		return nil, true
	}

	vb := reflect.ValueOf(dump.valueBuffer)
	v := vb.Index(dump.valueOffset).Interface()
	dump.valueOffset++

	return v, true
}
