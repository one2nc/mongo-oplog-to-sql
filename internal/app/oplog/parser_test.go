package oplog

import (
	"reflect"
	"testing"
)

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
				"CREATE TABLE test.student(_id VARCHAR(255) PRIMARY KEY, date_of_birth VARCHAR(255), is_graduated BOOLEAN, name VARCHAR(255), roll_no FLOAT);",
				"INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);"},
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
			want: []string{"UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';"},
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
			want: []string{"UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';"},
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
			want: []string{"UPDATE test.student SET is_graduated = true, roll_no = 50 WHERE _id = '635b79e231d82a8ab1de863b';"},
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
			want: []string{"DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := GenerateSQL(test.oplog)

			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("Generated SQL does not match the expected result.\nWant: %s\nGot: %s", test.want, got)
			}
		})
	}
}
