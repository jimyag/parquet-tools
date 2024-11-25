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

const (
	repeat = "  "
)

func init() {
	rootCmd.AddCommand(structCmd)
}

func structRun(cmd *cobra.Command, args []string) {
	rdrs, err := getReaders(args)
	if err != nil {
		log.Error(err).Msg("error getting readers")
		return
	}
	for _, rdr := range rdrs {
		parquetSchema := rdr.MetaData().Schema.Root()
		printGoStruct(parquetSchema, os.Stdout, 0)
	}
}

func printGoStruct(node *schema.GroupNode, w *os.File, depth int) {
	if depth == 0 {
		_, _ = w.WriteString(fmt.Sprintf("type %s struct {\n", node.Name()))
	}
	depth++
	indent := strings.Repeat(repeat, depth)
	for i := 0; i < node.NumFields(); i++ {
		field := node.Field(i)
		fieldName := field.Name()

		if group, ok := field.(*schema.GroupNode); ok {
			// 检查是否是简单的 List 结构
			if isSimpleList(group) {
				elementType := getListElementType(group)
				_, _ = fmt.Fprintf(w, "%s%s []%s `parquet:\"%s\"`\n",
					indent, toCamelCase(fieldName), elementType, fieldName)
			} else {
				// 嵌套结构处理
				_, _ = fmt.Fprintf(w, "%s%s struct {\n",
					indent, toCamelCase(fieldName))
				// 递归处理嵌套字段，增加缩进
				for j := 0; j < group.NumFields(); j++ {
					nestedField := group.Field(j)
					nestedName := nestedField.Name()
					indent = strings.Repeat(repeat, depth+1)
					nestedType := parquetTypeToGoType(nestedField)
					_, _ = fmt.Fprintf(w, "%s%s %s `parquet:\"%s\"`\n",
						indent, toCamelCase(nestedName), nestedType, nestedName)
					indent = strings.Repeat(repeat, depth)
				}
				// 结束嵌套结构体定义
				_, _ = fmt.Fprintf(w, "%s} `parquet:\"%s\"`\n",
					indent, fieldName)
			}
		} else {
			goType := parquetTypeToGoType(field)
			_, _ = fmt.Fprintf(w, "%s%s %s `parquet:\"%s\"`\n",
				indent, toCamelCase(fieldName), goType, fieldName)
		}
	}
	depth--
	indent = strings.Repeat(repeat, depth)
	if depth == 0 {
		_, _ = fmt.Fprintf(w, "%s}\n", indent)
	}
}

// 判断是否是简单的 List 结构（只包含一个 list 字段的结构）
func isSimpleList(group *schema.GroupNode) bool {
	return group.NumFields() == 1 &&
		group.Field(0).Name() == "list"
}

// 获取 List 的元素类型
func getListElementType(group *schema.GroupNode) string {
	if group.NumFields() == 1 {
		return parquetTypeToGoType(group.Field(0))
	}
	return "any"
}

// 辅助函数：将 parquet 数据类型转换为 Go 类型
func parquetTypeToGoType(field schema.Node) string {
	logicalType := field.LogicalType().String()
	switch {
	// String
	case strings.HasPrefix(logicalType, "String") || strings.HasPrefix(logicalType, "string"):
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
	case strings.HasPrefix(logicalType, "Decimal") || strings.HasPrefix(logicalType, "decimal"):
		return "float64"
	// Date
	case strings.HasPrefix(logicalType, "Date") || strings.HasPrefix(logicalType, "date"):
		return "time.Time"
	// Time fmt.Sprintf("Time(isAdjustedToUTC=%t, timeUnit=%s)", t.typ.GetIsAdjustedToUTC(), timeUnitToString(t.typ.GetUnit()))
	case strings.HasPrefix(logicalType, "Time") || strings.HasPrefix(logicalType, "time"):
		return "time.Time"
	// Timestamp fmt.Sprintf("Timestamp(isAdjustedToUTC=%t, timeUnit=%s, is_from_converted_type=%t, force_set_converted_type=%t)",t.typ.GetIsAdjustedToUTC(), timeUnitToString(t.typ.GetUnit()), t.fromConverted, t.forceConverted)
	case strings.HasPrefix(logicalType, "Timestamp") || strings.HasPrefix(logicalType, "timestamp"):
		return "time.Time"
		// Float16
	case strings.HasPrefix(logicalType, "Float") || strings.HasPrefix(logicalType, "float"):
		return "float32"
	case strings.HasPrefix(logicalType, "Double") || strings.HasPrefix(logicalType, "double"):
		return "float64"
	case strings.HasPrefix(logicalType, "Boolean") || strings.HasPrefix(logicalType, "boolean"):
		return "bool"
	case strings.HasPrefix(logicalType, "Binary") || strings.HasPrefix(logicalType, "binary"):
		return "[]byte"
	case strings.HasPrefix(logicalType, "JSON") || strings.HasPrefix(logicalType, "json"):
		return "json.RawMessage"
	case strings.HasPrefix(logicalType, "UUID") || strings.HasPrefix(logicalType, "uuid"):
		return "uuid.UUID" // github.com/google/uuid
	// List
	case strings.HasPrefix(logicalType, "List") || strings.HasPrefix(logicalType, "list"):
		// 如果是 List 类型，尝试获取元素类型
		if listNode, ok := field.(*schema.GroupNode); ok && listNode.NumFields() > 0 {
			elementField := listNode.Field(0)
			elementType := parquetTypeToGoType(elementField)
			return "[]" + elementType
		}
		return "[]any"
	// Map
	case strings.HasPrefix(logicalType, "Map") || strings.HasPrefix(logicalType, "map"):
		return "map[string]any"
	case strings.HasPrefix(logicalType, "Array") || strings.HasPrefix(logicalType, "array"):
		return "[]"
	case strings.HasPrefix(logicalType, "Struct") || strings.HasPrefix(logicalType, "struct"):
		return "struct"
	case strings.HasPrefix(logicalType, "Enum") || strings.HasPrefix(logicalType, "enum"):
		return "string"
	case strings.Contains(logicalType, "Interval") || strings.Contains(logicalType, "interval"):
		return "time.Duration"
	case strings.HasPrefix(logicalType, "Unknown") || strings.HasPrefix(logicalType, "unknown"):
		return "any"
	case strings.HasPrefix(logicalType, "Null") || strings.HasPrefix(logicalType, "null"):
		return "any"
	case strings.HasPrefix(logicalType, "None") || strings.HasPrefix(logicalType, "none"):
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
