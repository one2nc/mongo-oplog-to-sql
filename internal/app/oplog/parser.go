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
	O2 map[string]interface{} `json:"o2"`
}

func GenerateSQL(oplog OplogEntry) string {
	switch oplog.Op {
	case "i":
		return generateInsertSQL(oplog)
	case "u":
		return generateUpdateSQL(oplog)
	case "d":
		return generateDeleteSQL(oplog)
	default:
		return ""
	}
}

func generateInsertSQL(oplog OplogEntry) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(oplog.NS)
	sb.WriteString(" ")

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

	sb.WriteString(fmt.Sprintf("(%s)", strings.Join(columns, ", ")))
	sb.WriteString(" VALUES ")
	sb.WriteString(fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	sb.WriteString(";")

	return sb.String()
}

func generateUpdateSQL(oplog OplogEntry) string {
	diffMap, ok1 := oplog.O["diff"].(map[string]interface{})
	if !ok1 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(oplog.NS)
	sb.WriteString(" SET ")

	var setUnsetCols string
	if setMap, ok := diffMap["u"].(map[string]interface{}); ok {
		setUnsetCols = setClause(setMap)
	} else if unSetMap, ok := diffMap["d"].(map[string]interface{}); ok {
		setUnsetCols = unSetClause(unSetMap)
	} else {
		return ""
	}

	sb.WriteString(setUnsetCols)
	sb.WriteString(whereClause(oplog.O2))
	sb.WriteString(";")

	return sb.String()
}

func generateDeleteSQL(oplog OplogEntry) string {
	if len(oplog.O) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(oplog.NS)

	sb.WriteString(whereClause(oplog.O))
	sb.WriteString(";")

	return sb.String()
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

func setClause(cols map[string]interface{}) string {
	// Sort the column names of the oplog.O map to maintain the order in the update statement
	columnNames := sortColumns(cols)

	columns := make([]string, 0)
	for _, columnName := range columnNames {
		value := cols[columnName]
		columns = append(columns, fmt.Sprintf("%s = %s", columnName, getColumnValue(value)))
	}
	return strings.Join(columns, ", ")
}

func unSetClause(cols map[string]interface{}) string {
	// Sort the column names of the oplog.O map to maintain the order in the update statement
	columnNames := sortColumns(cols)

	columns := make([]string, 0)
	for _, columnName := range columnNames {
		columns = append(columns, fmt.Sprintf("%s = NULL", columnName))
	}
	return strings.Join(columns, ", ")
}

func whereClause(cols map[string]interface{}) string {
	var sb strings.Builder
	sb.WriteString(" WHERE ")
	// Sort the column names of the oplog.O map to maintain the order in the where clause
	columnNames := sortColumns(cols)

	columns := make([]string, 0)
	for _, columnName := range columnNames {
		value := cols[columnName]
		columns = append(columns, fmt.Sprintf("%s = %s", columnName, getColumnValue(value)))
	}

	sb.WriteString(strings.Join(columns, " AND "))
	return sb.String()
}

func sortColumns(cols map[string]interface{}) []string {
	columnNames := make([]string, 0)
	for columnName := range cols {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)
	return columnNames
}
