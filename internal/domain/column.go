package domain

import "fmt"

type Column struct {
	dataType string
	Name     string
	Value    interface{}
}

func (c Column) DataType() string {
	return getColumnSQLDataType(c.Name, c.Value)
}

func (c Column) PrimaryKey() bool {
	return c.Name == "_id"
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
	return columnDataType
}
