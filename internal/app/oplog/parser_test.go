package oplog

import (
	"testing"
)

func TestGenerateSQL(t *testing.T) {
	tests := []struct {
		name     string
		oplog    OplogEntry
		expected string
	}{
		{
			name:     "Empty Operation",
			oplog:    OplogEntry{},
			expected: "",
		},
		{
			name: "Insert Operation",
			oplog: OplogEntry{
				Op: "i",
				NS: "test.student",
				O: map[string]interface{}{
					"_id":           "635b79e231d82a8ab1de863b",
					"name":          "Selena Miller",
					"roll_no":       51,
					"is_graduated":  false,
					"date_of_birth": "2000-01-30",
				},
			},
			expected: "INSERT INTO test.student (_id, date_of_birth, is_graduated, name, roll_no) VALUES ('635b79e231d82a8ab1de863b', '2000-01-30', false, 'Selena Miller', 51);",
		},
		{
			name: "Update Operation - invalid diff oplog",
			oplog: OplogEntry{
				Op: "u",
				NS: "test.student",
				O: map[string]interface{}{
					"$v": 2,
					"invaliddiff": map[string]interface{}{
						"u": map[string]interface{}{
							"is_graduated": true,
						},
					},
				},
				O2: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: "",
		},
		{
			name: "Update Operation - invalid diff type oplog",
			oplog: OplogEntry{
				Op: "u",
				NS: "test.student",
				O: map[string]interface{}{
					"$v": 2,
					"diff": map[string]interface{}{
						"invalid": map[string]interface{}{
							"is_graduated": true,
						},
					},
				},
				O2: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: "",
		},
		{
			name: "Update Operation",
			oplog: OplogEntry{
				Op: "u",
				NS: "test.student",
				O: map[string]interface{}{
					"$v": 2,
					"diff": map[string]interface{}{
						"u": map[string]interface{}{
							"is_graduated": true,
						},
					},
				},
				O2: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: "UPDATE test.student SET is_graduated = true WHERE _id = '635b79e231d82a8ab1de863b';",
		},
		{
			name: "Update Operation - unset column",
			oplog: OplogEntry{
				Op: "u",
				NS: "test.student",
				O: map[string]interface{}{
					"$v": 2,
					"diff": map[string]interface{}{
						"d": map[string]interface{}{
							"roll_no": false,
						},
					},
				},
				O2: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: "UPDATE test.student SET roll_no = NULL WHERE _id = '635b79e231d82a8ab1de863b';",
		},
		{
			name: "Update Operation - update two columns",
			oplog: OplogEntry{
				Op: "u",
				NS: "test.student",
				O: map[string]interface{}{
					"$v": 2,
					"diff": map[string]interface{}{
						"u": map[string]interface{}{
							"roll_no":      50,
							"is_graduated": true,
						},
					},
				},
				O2: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: "UPDATE test.student SET is_graduated = true, roll_no = 50 WHERE _id = '635b79e231d82a8ab1de863b';",
		},
		{
			name: "Delete Operation - empty object",
			oplog: OplogEntry{
				Op: "d",
				NS: "test.student",
				O:  map[string]interface{}{},
			},
			expected: "",
		},
		{
			name: "Delete Operation",
			oplog: OplogEntry{
				Op: "d",
				NS: "test.student",
				O: map[string]interface{}{
					"_id": "635b79e231d82a8ab1de863b",
				},
			},
			expected: "DELETE FROM test.student WHERE _id = '635b79e231d82a8ab1de863b';",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sql := GenerateSQL(test.oplog)
			if sql != test.expected {
				t.Errorf("Generated SQL does not match the expected result.\nExpected: %s\nGot: %s", test.expected, sql)
			}
		})
	}
}
