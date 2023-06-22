package oplog

import (
	"fmt"
	"sort"
	"strings"
)

type OplogEntry struct {
	Op string                 `json:"op"`
	NS string                 `json:"ns"`
	O  map[string]interface{} `json:"o"`
}

func GenerateSQL(oplog OplogEntry) string {
	switch oplog.Op {
	case "i":
		return generateInsertSQL(oplog)
	default:
		return ""
	}
}

func generateInsertSQL(oplog OplogEntry) string {
	sql := fmt.Sprintf("INSERT INTO %s ", oplog.NS)
	columns := make([]string, 0)
	values := make([]string, 0)

	// Sort the column names of the oplog.O map to maintain the order in the insert statement
	columnNames := make([]string, 0, len(oplog.O))
	for columnName := range oplog.O {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)

	for _, columnName := range columnNames {
		columns = append(columns, columnName)

		value := oplog.O[columnName]
		values = append(values, getColumnValue(value))
	}

	sql += fmt.Sprintf("(%s) VALUES (%s);", strings.Join(columns, ", "), strings.Join(values, ", "))
	return sql
}

func getColumnValue(value interface{}) string {
	switch value.(type) {
	case int, int8, int16, int32, int64, float32, float64:
		return fmt.Sprintf("%v", value)
	case bool:
		return fmt.Sprintf("%t", value)
	default:
		return fmt.Sprintf("'%v'", value)
	}
}
