package service

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/one2nc/mongo-oplog-to-sql/internal/domain"
)

const STUBBED_ID = "stubbed-id"

type StubUUIDGenerator struct{}

func (g *StubUUIDGenerator) UUID() string {
	return STUBBED_ID
}

func TestGenerateSQL(t *testing.T) {
	tests := []struct {
		name  string
		oplog string
		want  []string
	}{
		{
			name:  "Empty Operation",
			oplog: `{}`,
			want:  []string{},
		},
		{
			name:  "Invalid Operation",
			oplog: ``,
			want:  []string{},
		},
		{
			name: "Insert Operation",
			oplog: `{
				"op": "i",
				"ns": "test.student",
				"o": {
				  "_id": "635b79e231d82a8ab1de863b",
				  "name": "Selena Miller",
				  "roll_no": 51,
				  "is_graduated": false,
				  "date_of_birth": "2000-01-30"
				}
			  }`,
			want: []string{
				"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
			},
		},
		{
			name: "Update Operation - invalid diff oplog",
			oplog: `{
				"op": "u",
				"ns": "test.student",
				"o": {
				   "$v": 2,
				   "invaliddiff": {
					  "u": {
						 "is_graduated": true
					  }
				   }
				},
				 "o2": {
				   "_id": "635b79e231d82a8ab1de863b"
				}
			 }`,
			want: []string{},
		},
		{
			name: "Update Operation - invalid diff type oplog",
			oplog: `{
				"op": "u",
				"ns": "test.student",
				"o": {
				   "$v": 2,
				   "diff": {
					  "invalid": {
						 "is_graduated": true
					  }
				   }
				},
				 "o2": {
				   "_id": "635b79e231d82a8ab1de863b"
				}
			 }`,
			want: []string{},
		},
		{
			name: "Update Operation",
			oplog: `{
				"op": "u",
				"ns": "test.student",
				"o": {
				   "$v": 2,
				   "diff": {
					  "u": {
						 "is_graduated": true
					  }
				   }
				},
				 "o2": {
				   "_id": "635b79e231d82a8ab1de863b"
				}
			 }`,
			want: []string{
				"UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';",
			},
		},
		{
			name: "Update Operation - unset column",
			oplog: `{
				"op": "u",
				"ns": "test.student",
				"o": {
				   "$v": 2,
				   "diff": {
					  "d": {
						 "roll_no": false
					  }
				   }
				},
				"o2": {
				   "_id": "635b79e231d82a8ab1de863b"
				}
			 }`,
			want: []string{
				"UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';",
			},
		},
		{
			name: "Update Operation - update two columns",
			oplog: `{
				"op": "u",
				"ns": "test.student",
				"o": {
				   "$v": 2,
				   "diff": {
					  "u": {
						 "roll_no": 50,
						 "is_graduated": true
					  }
				   }
				},
				"o2": {
				   "_id": "635b79e231d82a8ab1de863b"
				}
			 }`,
			want: []string{
				"UPDATE test.student SET is_graduated = true, roll_no = 50 WHERE _id = '635b79e231d82a8ab1de863b';",
			},
		},
		{
			name: "Delete Operation - empty object",
			oplog: `{
				"op": "d",
				"ns": "test.student",
				"o": {}
			  }`,
			want: []string{},
		},
		{
			name: "Delete Operation",
			oplog: `{
				"op": "d",
				"ns": "test.student",
				"o": {
				  "_id": "635b79e231d82a8ab1de863b"
				}
			  }`,
			want: []string{
				"DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';",
			},
		},
		{
			name: "Insert Operation - create table with multiple oplog entries",
			oplog: `[
				{
				  "op": "i",
				  "ns": "test.student",
				  "o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
				  }
				},
				{
				  "op": "i",
				  "ns": "test.student",
				  "o": {
					"_id": "14798c213f273a7ca2cf5174",
					"name": "George Smith",
					"roll_no": 21,
					"is_graduated": true,
					"date_of_birth": "2001-03-23"
				  }
				}
			  ]`,
			want: []string{
				"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', 21);",
			},
		},
		{
			name: "Insert Operation - create multiple tables with multiple oplog entries",
			oplog: `[
				{
				  "op": "i",
				  "ns": "test.student",
				  "o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
				  }
				},
				{
				  "op": "i",
				  "ns": "test.employee",
				  "o": {
					"_id": "14798c213f273a7ca2cf5174",
					"name": "George Smith",
					"salary": 10000,
					"is_graduated": true,
					"date_of_birth": "2001-03-23"
				  }
				}
			  ]`,
			want: []string{
				"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
				"CREATE TABLE test.employee (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), salary FLOAT);",
				"INSERT INTO test.employee (_id, date_of_birth, is_graduated, name, salary) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', 10000);",
			},
		},
		{
			name: "Insert Operation - alter table with multiple oplog entries",
			oplog: `[
				{
				  "op": "i",
				  "ns": "test.student",
				  "o": {
					"_id": "635b79e231d82a8ab1de863b",
					"name": "Selena Miller",
					"roll_no": 51,
					"is_graduated": false,
					"date_of_birth": "2000-01-30"
				  }
				},
				{
				  "op": "i",
				  "ns": "test.student",
				  "o": {
					"_id": "14798c213f273a7ca2cf5174",
					"name": "George Smith",
					"roll_no": 21,
					"is_graduated": true,
					"date_of_birth": "2001-03-23",
					"phone": "+91-81254966457"
				  }
				}
			  ]`,
			want: []string{
				"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
				"ALTER TABLE test.student ADD COLUMN phone VARCHAR(255);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, phone, roll_no) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', '+91-81254966457', 21);",
			},
		},
		{
			name: "Insert Operation - handle nested mongo document",
			oplog: `{
				"op": "i",
				"ns": "test.student",
				"o": {
				  "_id": "635b79e231d82a8ab1de863b",
				  "name": "Selena Miller",
				  "roll_no": 51,
				  "is_graduated": false,
				  "date_of_birth": "2000-01-30",
				  "address": [
					{
					  "line1": "481 Harborsburgh",
					  "zip": "89799"
					},
					{
					  "line1": "329 Flatside",
					  "zip": "80872"
					}
				  ],
				  "phone": {
					"personal": "7678456640",
					"work": "8130097989"
				  }
				}
			  }`,
			want: []string{
				"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
				"CREATE TABLE test.student_address (_id VARCHAR(255) PRIMARY KEY, student__id VARCHAR(255), line1 VARCHAR(255), zip VARCHAR(255));",
				"INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('stubbed-id', '481 Harborsburgh', '635b79e231d82a8ab1de863b', '89799');",
				"INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('stubbed-id', '329 Flatside', '635b79e231d82a8ab1de863b', '80872');",
				"CREATE TABLE test.student_phone (_id VARCHAR(255) PRIMARY KEY, student__id VARCHAR(255), personal VARCHAR(255), work VARCHAR(255));",
				"INSERT INTO test.student_phone (_id, personal, student__id, work) VALUES ('stubbed-id', '7678456640', '635b79e231d82a8ab1de863b', '8130097989');",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			uuidGenerator := &StubUUIDGenerator{}
			oplogService := NewOplogService(context.Background(), uuidGenerator)
			got := oplogService.ProcessOplog(test.oplog)

			if !reflect.DeepEqual(got, test.want) {
				t.Errorf(
					"Generated SQL does not match the expected result.\nWant: %s\nGot: %s",
					test.want,
					got,
				)
			}
		})
	}
}

func TestProcessOplogsConcurrent(t *testing.T) {
	tests := []struct {
		name       string
		oplogChan  chan domain.OplogEntry
		cancelFunc context.CancelFunc
		want       []string
	}{
		{
			name:       "Process valid Oplogs concurrently",
			oplogChan:  createValidOplogChannel(),
			cancelFunc: func() {},
			want: []string{
				"CREATE SCHEMA test;",
				"CREATE TABLE test.student (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
				"CREATE TABLE test.student_address (_id VARCHAR(255) PRIMARY KEY, student__id VARCHAR(255), line1 VARCHAR(255), zip VARCHAR(255));",
				"INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('stubbed-id', '481 Harborsburgh', '635b79e231d82a8ab1de863b', '89799');",
				"INSERT INTO test.student_address (_id, line1, student__id, zip) VALUES ('stubbed-id', '329 Flatside', '635b79e231d82a8ab1de863b', '80872');",
				"CREATE TABLE test.student_phone (_id VARCHAR(255) PRIMARY KEY, student__id VARCHAR(255), personal VARCHAR(255), work VARCHAR(255));",
				"INSERT INTO test.student_phone (_id, personal, student__id, work) VALUES ('stubbed-id', '7678456640', '635b79e231d82a8ab1de863b', '8130097989');",
				"CREATE TABLE test.employee (_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), salary FLOAT);",
				"INSERT INTO test.employee (_id, date_of_birth, is_graduated, name, salary) VALUES ('14798c213f273a7ca2cf5174', '2001-03-23', true, 'George Smith', 10000);",
			},
		},
		// Add more test cases here as needed
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			uuidGenerator := &StubUUIDGenerator{}
			oplogService := NewOplogService(ctx, uuidGenerator)

			sqlStmtChan := oplogService.ProcessOplogsConcurrent(test.oplogChan, test.cancelFunc)

			// Collect generated SQL statements
			gotSQLs := collectGeneratedSQL(sqlStmtChan)

			// Test for Correctness, Not Order
			if len(gotSQLs) != len(test.want) {
				t.Errorf(
					"Generated SQL does not match the expected result.\nWant: %s\nGot: %s",
					test.want,
					gotSQLs,
				)
			}

			for _, want := range test.want {
				found := false
				for _, got := range gotSQLs {
					if got == want {
						found = true
						break
					}
				}
				if !found {
					t.Errorf(
						"Generated SQL does not match the expected result.\nWant: %s\nGot: %s",
						test.want,
						gotSQLs,
					)
				}
			}

			cancel()
		})
	}
}

func createValidOplogChannel() chan domain.OplogEntry {
	// Create and return a channel with valid OplogEntry data
	oplogChan := make(chan domain.OplogEntry)

	go func() {
		jsonOplog := `[{
			"op": "i",
			"ns": "test.student",
			"o": {
			  "_id": "635b79e231d82a8ab1de863b",
			  "name": "Selena Miller",
			  "roll_no": 51,
			  "is_graduated": false,
			  "date_of_birth": "2000-01-30",
			  "address": [
				{
				  "line1": "481 Harborsburgh",
				  "zip": "89799"
				},
				{
				  "line1": "329 Flatside",
				  "zip": "80872"
				}
			  ],
			  "phone": {
				"personal": "7678456640",
				"work": "8130097989"
			  }
			}
		},
		{
			"op": "i",
			"ns": "test.employee",
			"o": {
			"_id": "14798c213f273a7ca2cf5174",
			"name": "George Smith",
			"salary": 10000,
			"is_graduated": true,
			"date_of_birth": "2001-03-23"
			}
		}]`

		var oplogEntries []domain.OplogEntry
		err := json.Unmarshal([]byte(jsonOplog), &oplogEntries)
		if err != nil {
			panic(err)
		}

		for _, oplog := range oplogEntries {
			oplogChan <- oplog
		}

		close(oplogChan)
	}()

	return oplogChan
}

func collectGeneratedSQL(sqlStmtChan chan domain.SQLStatement) []string {
	got := []string{}
	var wg sync.WaitGroup

outerForLoop:
	for {
		select {
		case sqlStmt, ok := <-sqlStmtChan:
			if !ok {
				break outerForLoop
			}
			wg.Add(1)
			go func(sqlStmt domain.SQLStatement) {
				defer wg.Done()

				sqlChan := sqlStmt.GetChannel()

			innerForLoop:
				for {
					select {
					case sql, ok := <-sqlChan:
						if !ok {
							break innerForLoop
						}
						got = append(got, sql)
					case <-time.After(2 * time.Second):
						break innerForLoop
					}
				}
			}(sqlStmt)
		case <-time.After(2 * time.Second):
			break outerForLoop
		}
	}
	wg.Wait()

	return got
}
