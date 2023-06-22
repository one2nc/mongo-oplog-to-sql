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
