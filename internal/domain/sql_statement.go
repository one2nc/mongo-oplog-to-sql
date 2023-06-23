package domain

type SQLStatement struct {
	dbName  string
	sqlChan chan string
}

func NewSQLStatement(dbName string) SQLStatement {
	return SQLStatement{
		dbName:  dbName,
		sqlChan: make(chan string, 100),
	}
}
func (s SQLStatement) GetDBName() string {
	return s.dbName
}

func (s SQLStatement) GetChannel() <-chan string {
	return s.sqlChan
}

func (s *SQLStatement) Publish(msg string) {
	s.sqlChan <- msg
}

func (s *SQLStatement) Close() {
	close(s.sqlChan)
}
