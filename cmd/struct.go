package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/apache/arrow/go/v17/parquet/schema"
	"github.com/jimyag/log"
	"github.com/spf13/cobra"
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
	indent := strings.Repeat("\t", depth)
	if depth == 0 {
		_, _ = w.WriteString(fmt.Sprintf("type %s struct {\n", node.Name()))
	}

	for i := 0; i < node.NumFields(); i++ {
		field := node.Field(i)
		fieldName := field.Name()

		if group, ok := field.(*schema.GroupNode); ok {
			// 对于嵌套结构，直接在字段定义中展开结构体
			_, _ = w.WriteString(fmt.Sprintf("%s%s struct {\n",
				indent, toCamelCase(fieldName)))
			// 递归处理嵌套字段
			for j := 0; j < group.NumFields(); j++ {
				nestedField := group.Field(j)
				nestedName := nestedField.Name()
				nestedType := parquetTypeToGoType(nestedField)
				_, _ = w.WriteString(fmt.Sprintf("%s\t%s %s `parquet:\"%s\"`\n",
					indent, toCamelCase(nestedName), nestedType, nestedName))
			}
			_, _ = w.WriteString(fmt.Sprintf("%s} `parquet:\"%s\"`\n",
				indent, fieldName))
		} else {
			goType := parquetTypeToGoType(field)
			_, _ = w.WriteString(fmt.Sprintf("%s%s %s `parquet:\"%s\"`\n",
				indent, toCamelCase(fieldName), goType, fieldName))
		}
	}

	if depth == 0 {
		_, _ = w.WriteString("}\n")
	}
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
	// 如果不包含下划线，只需要处理首字母
	if !strings.Contains(s, "_") {
		if len(s) == 0 {
			return s
		}
		// 将首字母转为大写
		return strings.ToUpper(s[:1]) + s[1:]
	}

	// 处理包含下划线的情况
	words := strings.Split(s, "_")
	for i := range words {
		if len(words[i]) > 0 {
			// 将每个单词的首字母转为大写
			words[i] = strings.ToUpper(words[i][:1]) + words[i][1:]
		}
	}
	return strings.Join(words, "")
}
