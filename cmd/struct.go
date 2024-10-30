package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/jimyag/log"
	"github.com/spf13/cobra"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var structCmd = &cobra.Command{
	Use:   "struct",
	Short: "print the go struct for a file",
	Run:   structRun,
}

func init() {
	rootCmd.AddCommand(structCmd)
}

func structRun(cmd *cobra.Command, args []string) {
	rdrs, err := getReaders(args)
	if err != nil {
		log.Panic(err).Msg("error getting readers")
	}
	for _, rdr := range rdrs {
		parquetSchema := rdr.MetaData().Schema.Root()
		printGoStruct(parquetSchema, os.Stdout)
	}
}

func printGoStruct(node *schema.GroupNode, w *os.File) {
	_, _ = w.WriteString(fmt.Sprintf("type %s struct {\n", node.Name()))
	for i := 0; i < node.NumFields(); i++ {
		field := node.Field(i)
		fieldName := field.Name()
		goType := parquetTypeToGoType(field)
		_, _ = w.WriteString(fmt.Sprintf("    %s %s `parquet:\"%s\"`\n",
			toCamelCase(fieldName), goType, fieldName))
	}
	_, _ = w.WriteString("}\n")
}

// 辅助函数：将 parquet 数据类型转换为 Go 类型
func parquetTypeToGoType(field schema.Node) string {
	logicalType := field.LogicalType().String()
	switch {
	// String
	case strings.HasPrefix(logicalType, "String"):
		return "string"
	// Int fmt.Sprintf("Int(bitWidth=%d, isSigned=%t)", t.typ.GetBitWidth(), t.typ.GetIsSigned())
	case strings.HasPrefix(logicalType, "Int"):
		// 解析 Int(bitWidth=32, isSigned=true) 这样的格式
		if strings.Contains(logicalType, "bitWidth=64") {
			if strings.Contains(logicalType, "isSigned=true") {
				return "int64"
			}
			return "uint64"
		}
		if strings.Contains(logicalType, "isSigned=true") {
			return "int32"
		}
		return "uint32"
	// Decimal 格式为：fmt.Sprintf("Decimal(precision=%d, scale=%d)", t.typ.Precision, t.typ.Scale)
	case strings.HasPrefix(logicalType, "Decimal"):
		return "float64"
	// Date
	case strings.HasPrefix(logicalType, "Date"):
		return "time.Time"
	// Time fmt.Sprintf("Time(isAdjustedToUTC=%t, timeUnit=%s)", t.typ.GetIsAdjustedToUTC(), timeUnitToString(t.typ.GetUnit()))
	case strings.HasPrefix(logicalType, "Time"):
		return "time.Time"
	// Timestamp fmt.Sprintf("Timestamp(isAdjustedToUTC=%t, timeUnit=%s, is_from_converted_type=%t, force_set_converted_type=%t)",t.typ.GetIsAdjustedToUTC(), timeUnitToString(t.typ.GetUnit()), t.fromConverted, t.forceConverted)
	case strings.HasPrefix(logicalType, "Timestamp"):
		return "time.Time"
	// Float16
	case strings.HasPrefix(logicalType, "Float"):
		return "float32"
	case strings.HasPrefix(logicalType, "Double"):
		return "float64"
	case strings.HasPrefix(logicalType, "Boolean"):
		return "bool"
	case strings.HasPrefix(logicalType, "Binary"):
		return "[]byte"
	case strings.HasPrefix(logicalType, "JSON"):
		return "json.RawMessage"
	case strings.HasPrefix(logicalType, "UUID"):
		return "string"
	// Map
	// List
	// Enum
	// Unknown
	// JSON
	// BSON
	// UUID
	// Interval
	// Null
	// None
	default:
		return "any"
	}
}

// 辅助函数：转换为驼峰命名
func toCamelCase(s string) string {
	words := strings.Split(s, "_")
	caser := cases.Title(language.Und)
	for i := range words {
		words[i] = caser.String(words[i])
	}
	return strings.Join(words, "")
}
