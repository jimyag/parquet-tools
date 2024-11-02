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
		printGoStruct(parquetSchema, os.Stdout, 0)
	}
}

func printGoStruct(node *schema.GroupNode, w *os.File, depth int) {
	indent := strings.Repeat("  ", depth)
	_, _ = w.WriteString(fmt.Sprintf("%stype %s struct {\n", indent, node.Name()))
	depth++
	indent = strings.Repeat("  ", depth)
	for i := 0; i < node.NumFields(); i++ {
		field := node.Field(i)
		field.Type()
		fieldName := field.Name()
		if group, ok := field.(*schema.GroupNode); ok {
			// 先定义嵌套的结构体
			printGoStruct(group, w, depth+1)
			// 在当前结构体中引用嵌套结构
			_, _ = w.WriteString(fmt.Sprintf("%s%s %s `parquet:\"%s\"`\n",
				indent, toCamelCase(fieldName), group.Name(), fieldName))
		} else {
			goType := parquetTypeToGoType(field)
			_, _ = w.WriteString(fmt.Sprintf("%s%s %s `parquet:\"%s\"`\n",
				indent, toCamelCase(fieldName), goType, fieldName))
		}
	}
	depth = depth - 1
	indent = strings.Repeat("  ", depth)
	_, _ = w.WriteString(fmt.Sprintf("%s}\n", indent))
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
		return "uuid.UUID" // github.com/google/uuid
	// List
	case strings.HasPrefix(logicalType, "List"):
		// 如果是 List 类型，尝试获取元素类型
		if listNode, ok := field.(*schema.GroupNode); ok && listNode.NumFields() > 0 {
			elementField := listNode.Field(0)
			elementType := parquetTypeToGoType(elementField)
			return "[]" + elementType
		}
		return "[]any"
	// Map
	case strings.HasPrefix(logicalType, "Map"):
		return "map[string]any"
	case strings.HasPrefix(logicalType, "Array"):
		return "[]"
	case strings.HasPrefix(logicalType, "Struct"):
		return "struct"
	case strings.HasPrefix(logicalType, "Enum"):
		return "string"
	case strings.Contains(logicalType, "Interval"):
		return "time.Duration"
	case strings.HasPrefix(logicalType, "Unknown"):
		return "any"
	case strings.HasPrefix(logicalType, "Null"):
		return "any"
	case strings.HasPrefix(logicalType, "None"):
		return "any"
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
