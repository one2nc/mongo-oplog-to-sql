package service

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/one2nc/mongo-oplog-to-sql/internal/domain"
)

type OplogService interface {
	GenerateSQL(oplog string) []string
	ProcessOplogs(oplogChan <-chan domain.OplogEntry, cancel context.CancelFunc) <-chan string
}

type oplogService struct {
	ctx context.Context

	cacheMap      map[string]bool
	uuidGenerator domain.UUIDGenerator
}

func NewOplogService(ctx context.Context, uuidGenerator domain.UUIDGenerator) OplogService {
	return &oplogService{
		ctx:           ctx,
		cacheMap:      make(map[string]bool),
		uuidGenerator: uuidGenerator,
	}
}

func (s *oplogService) ProcessOplogs(
	oplogChan <-chan domain.OplogEntry,
	cancel context.CancelFunc,
) <-chan string {
	sqlChan := make(chan string, 100)

	go func() {
	forLoop:
		for entry := range oplogChan {
			// Check if the context is done
			select {
			case <-s.ctx.Done():
				// The context is done, stop reading Oplogs
				break forLoop
			default:
				// Context is still active, continue reading Oplogs
			}

			sqls, err := s.generateSQL(entry)
			if err != nil {
				break
			}

			for _, sql := range sqls {
				sqlChan <- sql
			}
		}

		// Close the out channel after all values are processed
		close(sqlChan)

		cancel()
	}()

	return sqlChan
}

func (s *oplogService) GenerateSQL(oplogString string) []string {
	var oplogEntries []domain.OplogEntry
	err := json.Unmarshal([]byte(oplogString), &oplogEntries)
	if err != nil {
		var oplogEntry domain.OplogEntry
		err := json.Unmarshal([]byte(oplogString), &oplogEntry)
		if err != nil {
			return []string{}
		}
		oplogEntries = append(oplogEntries, oplogEntry)
	}

	sqlStatements := make([]string, 0)
	for _, entry := range oplogEntries {
		sqls, err := s.generateSQL(entry)
		if err != nil {
			break
		}
		sqlStatements = append(sqlStatements, sqls...)
	}

	return sqlStatements
}

func (s *oplogService) generateSQL(entry domain.OplogEntry) ([]string, error) {
	sqlStatements := []string{}
	switch entry.Operation {
	case "i":
		nsParts := strings.Split(entry.Namespace, ".")
		schemaName := nsParts[0]
		if !s.cacheMap[schemaName] {
			s.cacheMap[schemaName] = true
			sqlStatements = append(sqlStatements, generateCreateSchemaSQL(schemaName))
		}

		sqlStatements = append(
			sqlStatements,
			s.generateCreateAlterAndInsertSQL(entry.Namespace, domain.Column{}, entry.Object)...)
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

func (s *oplogService) generateCreateAlterAndInsertSQL(
	namespace string,
	foreignColumn domain.Column,
	data map[string]interface{},
) []string {
	sqlStatements := []string{}

	// create schema if not exists
	if !s.cacheMap[namespace] {
		s.cacheMap[namespace] = true
		sqlStatements = append(
			sqlStatements,
			s.generateCreateTableSQL(namespace, foreignColumn, data),
		)
	} else if s.isEligibleForAlterTable(namespace, data) { // alter table if applicable
		sqlStatements = append(sqlStatements, s.generateAlterTableSQL(namespace, data))
	}

	// add foreign column in the data
	if foreignColumn.Name != "" {
		data[foreignColumn.Name] = foreignColumn.Value
	}

	// generate insert statement
	sqlStatements = append(sqlStatements, s.generateInsertSQL(namespace, data))

	nsParts := strings.Split(namespace, ".")
	schema := nsParts[0]
	collection := nsParts[1]

	// generate SQL statements for nested objects or arrays of objects
	columnNames := sortColumns(data)
	for _, columnName := range columnNames {
		value := data[columnName]

		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			foreignTableName := columnName
			foreignColumn := domain.Column{
				Name:  collection + "__id",
				Value: data["_id"],
			}

			sqlStatements = append(
				sqlStatements,
				s.generateSQLForNestedObject(schema, foreignTableName, foreignColumn, value)...)
		default:
			continue
		}
	}

	return sqlStatements
}

func generateCreateSchemaSQL(schemaName string) string {
	return fmt.Sprintf("CREATE SCHEMA %s;", schemaName)
}

func (s *oplogService) generateCreateTableSQL(
	tableName string,
	foreignColumn domain.Column,
	data map[string]interface{},
) string {
	var sb strings.Builder
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(tableName)
	sb.WriteString(" (")

	columnNames := sortColumns(data)

	sep := ""
	if foreignColumn.Name != "" {
		// create primary key for sub table
		primaryKeyCol := domain.Column{
			Name:  "_id",
			Value: "",
		}
		sb.WriteString(createColumn(tableName, sep, primaryKeyCol, s.cacheMap))
		sep = ", "

		// create foreign key in the sub table
		sb.WriteString(createColumn(tableName, sep, foreignColumn, s.cacheMap))
		sep = ", "
	}

	for _, columnName := range columnNames {
		value := data[columnName]

		// skip nested objects or arrays of objects
		skip := false
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			skip = true
		}
		if !skip {
			column := domain.Column{Name: columnName, Value: value}

			columnDataType := column.DataType()
			cacheKey := fmt.Sprintf("%s.%s.%s", tableName, columnName, columnDataType)
			s.cacheMap[cacheKey] = true

			sb.WriteString(fmt.Sprintf("%s%s %s", sep, columnName, columnDataType))
			if column.PrimaryKey() {
				sb.WriteString(" PRIMARY KEY")
			}
			sep = ", "
		}
	}

	sb.WriteString(");")
	return sb.String()
}

func createColumn(tableName, sep string, column domain.Column, cacheMap map[string]bool) string {
	cacheKey := fmt.Sprintf("%s.%s.%s", tableName, column.Name, column.DataType())
	cacheMap[cacheKey] = true

	if column.PrimaryKey() {
		return fmt.Sprintf("%s%s %s PRIMARY KEY", sep, column.Name, column.DataType())
	}
	return fmt.Sprintf("%s%s %s", sep, column.Name, column.DataType())
}

func (s *oplogService) generateSQLForNestedObject(
	schema, tableName string,
	foreignColumn domain.Column,
	value interface{},
) []string {
	namespace := fmt.Sprintf("%s.%s", schema, tableName)

	switch reflect.TypeOf(value).Kind() {
	case reflect.Slice:
		sqlStatements := []string{}
		entries := value.([]interface{})
		for _, entry := range entries {
			subData := entry.(map[string]interface{})
			subStatements := s.generateCreateAlterAndInsertSQL(namespace, foreignColumn, subData)
			sqlStatements = append(sqlStatements, subStatements...)
		}
		return sqlStatements
	case reflect.Map:
		subData := value.(map[string]interface{})
		return s.generateCreateAlterAndInsertSQL(namespace, foreignColumn, subData)
	}

	return []string{}
}

func (s *oplogService) isEligibleForAlterTable(namespace string, data map[string]interface{}) bool {
	for columnName, value := range data {
		// skip nested objects or arrays of objects
		skip := false
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			skip = true
		}
		if !skip {
			column := domain.Column{Name: columnName, Value: value}
			columnDataType := column.DataType()

			cacheKey := fmt.Sprintf("%s.%s.%s", namespace, columnName, columnDataType)
			if !s.cacheMap[cacheKey] {
				return true
			}
		}
	}
	return false
}

func (s *oplogService) generateAlterTableSQL(namespace string, data map[string]interface{}) string {
	var sb strings.Builder
	sb.WriteString("ALTER TABLE ")
	sb.WriteString(namespace)

	sep := " "
	for columnName, value := range data {
		column := domain.Column{Name: columnName, Value: value}
		columnDataType := column.DataType()

		cacheKey := fmt.Sprintf("%s.%s.%s", namespace, columnName, columnDataType)
		if !s.cacheMap[cacheKey] {
			s.cacheMap[cacheKey] = true
			sb.WriteString(fmt.Sprintf("%sADD COLUMN %s %s", sep, columnName, columnDataType))
			sep = ", "
		}
	}

	sb.WriteString(";")
	return sb.String()
}

func (s *oplogService) generateInsertSQL(tableName string, data map[string]interface{}) string {
	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" ")

	// add primary key which is not exists for sub table
	if _, exists := data["_id"]; !exists {
		data["_id"] = s.uuidGenerator.UUID()
	}

	columns := make([]string, 0, len(data))
	values := make([]string, 0, len(data))

	columnNames := sortColumns(data)
	for _, columnName := range columnNames {
		value := data[columnName]
		// skip nested objects or arrays of objects
		skip := false
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			skip = true
		}
		if !skip {
			columns = append(columns, columnName)

			values = append(values, getColumnValue(value))
		}
	}

	sb.WriteString(fmt.Sprintf("(%s)", strings.Join(columns, ", ")))
	sb.WriteString(" VALUES ")
	sb.WriteString(fmt.Sprintf("(%s)", strings.Join(values, ", ")))
	sb.WriteString(";")

	return sb.String()
}

func generateUpdateSQL(entry domain.OplogEntry) (string, error) {
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

func generateDeleteSQL(entry domain.OplogEntry) (string, error) {
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
