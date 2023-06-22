package oplog

import (
	"encoding/json"
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

func GenerateSQL(oplog string) []string {
	var oplogObj OplogEntry
	if err := json.Unmarshal([]byte(oplog), &oplogObj); err != nil {
		return []string{}
	}

	return generateSQL(oplogObj)
}

func generateSQL(oplog OplogEntry) []string {
	sqls := []string{}
	switch oplog.Op {
	case "i":
		sqls = append(sqls, generateCreateSchemaSQL(oplog.NS))
		sqls = append(sqls, generateCreateTableSQL(oplog))
		sqls = append(sqls, generateInsertSQL(oplog))
	case "u":
		if sql, err := generateUpdateSQL(oplog); err == nil {
			sqls = append(sqls, sql)
		}
	case "d":
		if sql, err := generateDeleteSQL(oplog); err == nil {
			sqls = append(sqls, sql)
		}
	}
	return sqls
}

func generateCreateSchemaSQL(ns string) string {
	nsParts := strings.Split(ns, ".")
	return fmt.Sprintf("CREATE SCHEMA %s;", nsParts[0])
}

func generateCreateTableSQL(oplog OplogEntry) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(oplog.NS)
	sb.WriteRune('(')

	// Sort the column names of the oplog.O map to maintain the order in the insert statement
	columnNames := make([]string, 0, len(oplog.O))
	for columnName := range oplog.O {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)

	sep := ""
	for _, columnName := range columnNames {
		value := oplog.O[columnName]
		colDataType := getColumnSQLDatatype(columnName, value)

		sb.WriteString(fmt.Sprintf("%s%s %s", sep, columnName, colDataType))
		sep = ", "
	}

	sb.WriteString(");")
	return sb.String()
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

func generateUpdateSQL(oplog OplogEntry) (string, error) {
	diffMap, ok1 := oplog.O["diff"].(map[string]interface{})
	if !ok1 {
		return "", fmt.Errorf("invalid diff oplog")
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
		return "", fmt.Errorf("invalid operation in diff oplog")
	}

	sb.WriteString(setUnsetCols)
	sb.WriteString(whereClause(oplog.O2))
	sb.WriteString(";")

	return sb.String(), nil
}

func generateDeleteSQL(oplog OplogEntry) (string, error) {
	if len(oplog.O) == 0 {
		return "", fmt.Errorf("invalid oplog")
	}

	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(oplog.NS)

	sb.WriteString(whereClause(oplog.O))
	sb.WriteString(";")

	return sb.String(), nil
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

func getColumnSQLDatatype(colName string, value interface{}) string {
	var colDataType string
	switch value.(type) {
	case int, int8, int16, int32, int64:
		colDataType = "INTEGER"
	case float32, float64:
		colDataType = "FLOAT"
	case bool:
		colDataType = "BOOLEAN"
	default:
		colDataType = "VARCHAR(255)"
	}

	if colName == "_id" {
		colDataType = fmt.Sprintf("%s PRIMARY KEY", colDataType)
	}
	return colDataType
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
