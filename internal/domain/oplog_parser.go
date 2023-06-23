package domain

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
)

type OplogParser struct {
	uuidGenerator UUIDGenerator
}

func NewOplogParser(uuidGenerator UUIDGenerator) *OplogParser {
	return &OplogParser{
		uuidGenerator: uuidGenerator,
	}
}

func (p *OplogParser) ProcessCollectionOplog(
	oplogChan <-chan OplogEntry,
	sqlStmt SQLStatement,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	collectionCache := NewCache()
	tableMap := make(map[string]chan OplogEntry)

	// WaitGroup for tables
	var wgTable sync.WaitGroup
	for oplog := range oplogChan {
		tableName := oplog.TableName()
		if _, ok := tableMap[tableName]; !ok {
			tableChan := make(chan OplogEntry, 1000)
			tableMap[tableName] = tableChan
			wgTable.Add(1)

			go p.processTableOplog(tableChan, collectionCache, &wgTable, sqlStmt)

		}
		tableMap[tableName] <- oplog
	}

	for _, tableOplogChan := range tableMap {
		close(tableOplogChan)
	}

	wgTable.Wait()
}

func (p *OplogParser) processTableOplog(
	oplogChan <-chan OplogEntry,
	cache Cache,
	wg *sync.WaitGroup,
	sqlStmt SQLStatement,
) {
	defer wg.Done()

	for entry := range oplogChan {
		// process the Oplog entry
		sqls, err := p.ProcessOplog(entry, cache)
		if err != nil {
			break
		}

		for _, sql := range sqls {
			sqlStmt.Publish(sql)
		}
	}
}

func (p *OplogParser) ProcessOplog(entry OplogEntry, cache Cache) ([]string, error) {
	sqlStatements := []string{}
	switch entry.Operation {
	case "i":
		nsParts := strings.Split(entry.Namespace, ".")
		schemaName := nsParts[0]
		// create table if not exists
		if !cache.LoadOrStore(schemaName, true) {
			sqlStatements = append(sqlStatements, generateCreateSchemaSQL(schemaName))
		}

		sqlStatements = append(
			sqlStatements,
			p.generateTableAndInsertSQL(
				entry.Namespace,
				cache,
				nil,
				entry.Object,
			)...)
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

func (p *OplogParser) generateTableAndInsertSQL(
	namespace string,
	cache Cache,
	foreignColumn *Column,
	data map[string]interface{},
) []string {
	sqlStatements := []string{}

	// create table if not exists
	if !cache.LoadOrStore(namespace, true) {
		sqlStatements = append(
			sqlStatements,
			generateCreateTableSQL(namespace, cache, foreignColumn, data),
		)
	} else if isEligibleForAlterTable(namespace, cache, data) { // alter table if applicable
		sqlStatements = append(sqlStatements, generateAlterTableSQL(namespace, cache, data))
	}

	// add foreign column in the data
	if foreignColumn != nil {
		data[foreignColumn.Name] = foreignColumn.Value
	}

	// generate insert statement
	sqlStatements = append(sqlStatements, p.generateInsertSQL(namespace, data))

	nsParts := strings.Split(namespace, ".")
	schema := nsParts[0]
	collection := nsParts[1]

	// generate SQL statements for nested objects or arrays of objects
	columnNames := sortColumns(data)
	for _, columnName := range columnNames {
		value := data[columnName]

		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			foreignTableName := fmt.Sprintf("%s_%s", collection, columnName)
			foreignColumn := &Column{
				Name:  collection + "__id",
				Value: data["_id"],
			}

			sqlStatements = append(
				sqlStatements,
				p.generateSQLForNestedObject(
					schema,
					cache,
					foreignTableName,
					foreignColumn,
					value,
				)...)
		default:
			continue
		}
	}

	return sqlStatements
}

func generateCreateTableSQL(
	tableName string,
	cache Cache,
	foreignColumn *Column,
	data map[string]interface{},
) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (", tableName))

	columnNames := sortColumns(data)

	sep := ""
	if foreignColumn != nil {
		// create primary key for sub table
		primaryKeyCol := Column{
			Name:  "_id",
			Value: "",
		}
		sb.WriteString(createColumn(tableName, primaryKeyCol, cache))
		sep = ", "
		// create foreign key in the sub table
		sb.WriteString(fmt.Sprintf("%s%s", sep, createColumn(tableName, *foreignColumn, cache)))
	}

	for _, columnName := range columnNames {
		value := data[columnName]

		// skip nested objects or arrays of objects
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			continue
		}

		column := Column{Name: columnName, Value: value}

		sb.WriteString(fmt.Sprintf("%s%s", sep, createColumn(tableName, column, cache)))
		sep = ", "
	}

	sb.WriteString(");")
	return sb.String()
}

func createColumn(tableName string, column Column, cache Cache) string {
	cacheKey := fmt.Sprintf("%s.%s.%s", tableName, column.Name, column.DataType())
	cache.LoadOrStore(cacheKey, true)

	if column.PrimaryKey() {
		return fmt.Sprintf("%s %s PRIMARY KEY", column.Name, column.DataType())
	}
	return fmt.Sprintf("%s %s", column.Name, column.DataType())
}

func (p *OplogParser) generateSQLForNestedObject(
	schema string,
	cache Cache,
	tableName string,
	foreignColumn *Column,
	value interface{},
) []string {
	namespace := fmt.Sprintf("%s.%s", schema, tableName)

	switch reflect.TypeOf(value).Kind() {
	case reflect.Slice:
		sqlStatements := []string{}
		entries := value.([]interface{})
		for _, entry := range entries {
			subData := entry.(map[string]interface{})
			subStatements := p.generateTableAndInsertSQL(
				namespace,
				cache,
				foreignColumn,
				subData,
			)
			sqlStatements = append(sqlStatements, subStatements...)
		}
		return sqlStatements
	case reflect.Map:
		subData := value.(map[string]interface{})
		return p.generateTableAndInsertSQL(namespace, cache, foreignColumn, subData)
	}

	return []string{}
}

func isEligibleForAlterTable(
	namespace string,
	cache Cache,
	data map[string]interface{},
) bool {
	for columnName, value := range data {
		// skip nested objects or arrays of objects
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			continue
		}
		column := Column{Name: columnName, Value: value}
		columnDataType := column.DataType()

		cacheKey := fmt.Sprintf("%s.%s.%s", namespace, columnName, columnDataType)
		if !cache.Get(cacheKey) {
			return true
		}
	}
	return false
}

func generateAlterTableSQL(
	namespace string,
	cache Cache,
	data map[string]interface{},
) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("ALTER TABLE %s", namespace))

	sep := " "
	for columnName, value := range data {
		// skip nested objects or arrays of objects
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			continue
		}
		column := Column{Name: columnName, Value: value}
		columnDataType := column.DataType()

		cacheKey := fmt.Sprintf("%s.%s.%s", namespace, columnName, columnDataType)
		if !cache.LoadOrStore(cacheKey, true) {
			sb.WriteString(fmt.Sprintf("%sADD COLUMN %s %s", sep, columnName, columnDataType))
			sep = ", "
		}
	}

	sb.WriteString(";")
	return sb.String()
}

func (p *OplogParser) generateInsertSQL(tableName string, data map[string]interface{}) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s ", tableName))

	// add primary key which is not exists for sub table
	if _, exists := data["_id"]; !exists {
		data["_id"] = p.uuidGenerator.UUID()
	}

	columns := make([]string, 0, len(data))
	values := make([]string, 0, len(data))

	columnNames := sortColumns(data)
	for _, columnName := range columnNames {
		value := data[columnName]
		// skip nested objects or arrays of objects
		switch reflect.TypeOf(value).Kind() {
		case reflect.Slice, reflect.Map:
			continue
		}

		columns = append(columns, columnName)
		values = append(values, getColumnValue(value))
	}

	sb.WriteString(fmt.Sprintf("(%s) VALUES (%s);", strings.Join(columns, ", "), strings.Join(values, ", ")))
	return sb.String()
}

func generateUpdateSQL(entry OplogEntry) (string, error) {
	diffMap, ok := entry.Object["diff"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("invalid diff oplog")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("UPDATE %s SET ", entry.Namespace))

	var setUnsetCols string
	if setMap, ok := diffMap["u"].(map[string]interface{}); ok {
		setUnsetCols = generateSetClause(setMap)
	} else if unsetMap, ok := diffMap["d"].(map[string]interface{}); ok {
		setUnsetCols = generateUnsetClause(unsetMap)
	} else {
		return "", fmt.Errorf("invalid operation in diff oplog")
	}

	sb.WriteString(fmt.Sprintf("%s %s;", setUnsetCols, generateWhereClause(entry.Object2)))
	return sb.String(), nil
}

func generateDeleteSQL(entry OplogEntry) (string, error) {
	if len(entry.Object) == 0 {
		return "", fmt.Errorf("invalid oplog")
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("DELETE FROM %s %s;", entry.Namespace, generateWhereClause(entry.Object)))
	return sb.String(), nil
}

func generateSetClause(setMap map[string]interface{}) string {
	setCols := getSortedCols(setMap)
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
	whereCols := getSortedCols(whereMap)
	return fmt.Sprintf("WHERE %s", strings.Join(whereCols, " AND "))
}

func getSortedCols(data map[string]interface{}) []string {
	columnValues := make([]string, 0, len(data))
	for columnName, value := range data {
		columnValues = append(columnValues, fmt.Sprintf("%s = %s", columnName, getColumnValue(value)))
	}
	sort.Strings(columnValues)
	return columnValues
}

func sortColumns(cols map[string]interface{}) []string {
	columnNames := make([]string, 0)
	for columnName := range cols {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)
	return columnNames
}
