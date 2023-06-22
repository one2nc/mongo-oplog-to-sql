package oplog

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

type OplogEntry struct {
	Operation string                 `json:"op"`
	Namespace string                 `json:"ns"`
	Object    map[string]interface{} `json:"o"`
	Object2   map[string]interface{} `json:"o2"`
}

func GenerateSQL(oplogString string) []string {
	var oplogEntries []OplogEntry
	err := json.Unmarshal([]byte(oplogString), &oplogEntries)
	if err != nil {
		var oplogEntry OplogEntry
		err := json.Unmarshal([]byte(oplogString), &oplogEntry)
		if err != nil {
			return []string{}
		}
		oplogEntries = append(oplogEntries, oplogEntry)
	}

	cacheMap := make(map[string]bool)
	sqlStatements := make([]string, 0)
	for _, entry := range oplogEntries {
		sqls, err := generateSQLStatements(entry, cacheMap)
		if err != nil {
			break
		}
		sqlStatements = append(sqlStatements, sqls...)
	}

	return sqlStatements
}

func generateSQLStatements(entry OplogEntry, cacheMap map[string]bool) ([]string, error) {
	sqlStatements := []string{}
	switch entry.Operation {
	case "i":
		nsParts := strings.Split(entry.Namespace, ".")
		schemaName := nsParts[0]
		if !cacheMap[schemaName] {
			cacheMap[schemaName] = true
			sqlStatements = append(sqlStatements, generateCreateSchemaSQL(schemaName))
		}

		if !cacheMap[entry.Namespace] {
			cacheMap[entry.Namespace] = true
			sqlStatements = append(sqlStatements, generateCreateTableSQL(entry, cacheMap))
		} else if isEligibleForAlterTable(entry, cacheMap) {
			sqlStatements = append(sqlStatements, generateAlterTableSQL(entry, cacheMap))
		}

		sqlStatements = append(sqlStatements, generateInsertSQL(entry))
	case "u":
		if sql, err := generateUpdateSQL(entry); err == nil {
			sqlStatements = append(sqlStatements, sql)
		}
	case "d":
		if sql, err := generateDeleteSQL(entry); err == nil {
			sqlStatements = append(sqlStatements, sql)
		}
	}

	if len(sqlStatements) == 0 {
		return []string{}, fmt.Errorf("invalid oplog")
	}

	return sqlStatements, nil
}

func generateCreateSchemaSQL(schemaName string) string {
	return fmt.Sprintf("CREATE SCHEMA %s;", schemaName)
}

func generateCreateTableSQL(entry OplogEntry, cacheMap map[string]bool) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(entry.Namespace)
	sb.WriteString("(")

	columnNames := make([]string, 0, len(entry.Object))
	for columnName := range entry.Object {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)

	sep := ""
	for _, columnName := range columnNames {
		value := entry.Object[columnName]
		columnDataType := getColumnSQLDataType(columnName, value)

		cacheKey := fmt.Sprintf("%s.%s.%s", entry.Namespace, columnName, columnDataType)
		cacheMap[cacheKey] = true

		sb.WriteString(fmt.Sprintf("%s%s %s", sep, columnName, columnDataType))
		sep = ", "
	}

	sb.WriteString(");")
	return sb.String()
}

func isEligibleForAlterTable(entry OplogEntry, cacheMap map[string]bool) bool {
	for columnName := range entry.Object {
		value := entry.Object[columnName]
		columnDataType := getColumnSQLDataType(columnName, value)

		cacheKey := fmt.Sprintf("%s.%s.%s", entry.Namespace, columnName, columnDataType)
		if !cacheMap[cacheKey] {
			return true
		}
	}
	return false
}

func generateAlterTableSQL(entry OplogEntry, cacheMap map[string]bool) string {
	var sb strings.Builder
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(entry.Namespace)

	sep := " "
	for columnName := range entry.Object {
		value := entry.Object[columnName]
		columnDataType := getColumnSQLDataType(columnName, value)

		cacheKey := fmt.Sprintf("%s.%s.%s", entry.Namespace, columnName, columnDataType)
		if !cacheMap[cacheKey] {
			sb.WriteString(fmt.Sprintf("%sADD COLUMN %s %s", sep, columnName, columnDataType))
			sep = ", "
		}
	}

	sb.WriteString(";")
	return sb.String()
}

func generateInsertSQL(entry OplogEntry) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(entry.Namespace)
	sb.WriteString(" ")

	columns := make([]string, 0, len(entry.Object))
	values := make([]string, 0, len(entry.Object))

	columnNames := sortColumns(entry.Object)
	for _, columnName := range columnNames {
		columns = append(columns, columnName)

		value := entry.Object[columnName]
		values = append(values, getColumnValue(value))
	}

	sb.WriteString(fmt.Sprintf("(%s)", strings.Join(columns, ", ")))
	sb.WriteString(" VALUES ")
	sb.WriteString(fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	sb.WriteString(";")

	return sb.String()
}

func generateUpdateSQL(entry OplogEntry) (string, error) {
	diffMap, ok := entry.Object["diff"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid diff oplog")
	}

	var sb strings.Builder
	sb.WriteString("UPDATE ")
	sb.WriteString(entry.Namespace)
	sb.WriteString(" SET ")

	var setUnsetCols string
	if setMap, ok := diffMap["u"].(map[string]interface{}); ok {
		setUnsetCols = generateSetClause(setMap)
	} else if unsetMap, ok := diffMap["d"].(map[string]interface{}); ok {
		setUnsetCols = generateUnsetClause(unsetMap)
	} else {
		return "", fmt.Errorf("invalid operation in diff oplog")
	}

	sb.WriteString(setUnsetCols)
	sb.WriteString(generateWhereClause(entry.Object2))
	sb.WriteString(";")

	return sb.String(), nil
}

func generateDeleteSQL(entry OplogEntry) (string, error) {
	if len(entry.Object) == 0 {
		return "", fmt.Errorf("invalid oplog")
	}

	var sb strings.Builder
	sb.WriteString("DELETE FROM ")
	sb.WriteString(entry.Namespace)

	sb.WriteString(generateWhereClause(entry.Object))
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

func getColumnSQLDataType(columnName string, value interface{}) string {
	var columnDataType string
	switch value.(type) {
	case int, int8, int16, int32, int64:
		columnDataType = "INTEGER"
	case float32, float64:
		columnDataType = "FLOAT"
	case bool:
		columnDataType = "BOOLEAN"
	default:
		// For simplicity, treat all non-numeric values as string
		columnDataType = "VARCHAR(255)"
	}

	if columnName == "_id" {
		columnDataType = fmt.Sprintf("%s PRIMARY KEY", columnDataType)
	}
	return columnDataType
}

func generateSetClause(setMap map[string]interface{}) string {
	setColNames := sortColumns(setMap)
	var setCols []string
	for _, columnName := range setColNames {
		value := setMap[columnName]
		setCols = append(setCols, fmt.Sprintf("%s = %s", columnName, getColumnValue(value)))
	}
	return strings.Join(setCols, ", ")
}

func generateUnsetClause(unsetMap map[string]interface{}) string {
	unsetColNames := sortColumns(unsetMap)
	var unsetCols []string
	for _, columnName := range unsetColNames {
		unsetCols = append(unsetCols, fmt.Sprintf("%s = NULL", columnName))
	}
	return strings.Join(unsetCols, ", ")
}

func generateWhereClause(whereMap map[string]interface{}) string {
	whereColNames := sortColumns(whereMap)
	var whereCols []string
	for _, columnName := range whereColNames {
		value := whereMap[columnName]
		whereCols = append(whereCols, fmt.Sprintf("%s = %s", columnName, getColumnValue(value)))
	}
	return fmt.Sprintf(" WHERE %s", strings.Join(whereCols, " AND "))
}

func sortColumns(cols map[string]interface{}) []string {
	columnNames := make([]string, 0)
	for columnName := range cols {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)
	return columnNames
}
