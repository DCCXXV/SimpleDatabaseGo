package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestIntegration_InsertAndSelect(t *testing.T) {
	var tableFull strings.Builder
	for i := 1; i <= TABLE_MAX_ROWS+1; i++ {
		tableFull.WriteString(fmt.Sprintf("insert %d user%d person%d@example.com\n", i, i, i))
	}
	tableFull.WriteString("+quit\n")

	tests := []struct {
		name         string
		input        string
		wantContains []string
		wantRows     int
	}{
		{
			name: "inserts and retrieves a row",
			input: `insert 1 user1 person1@example.com
			select
			+quit
			`,
			wantContains: []string{
				"(1, user1, person1@example.com)",
				"Executed.",
			},
			wantRows: 1,
		},
		{
			name:  "prints error message when table is full",
			input: tableFull.String(),
			wantContains: []string{
				"Error: Table full.",
			},
			wantRows: TABLE_MAX_ROWS,
		},
		{
			name: "allows inserting strings that are the maximun length",
			input: fmt.Sprintf(`insert 1 %s %s
			select
			+quit`, strings.Repeat("a", COLUMN_USERNAME_SIZE), strings.Repeat("a", COLUMN_EMAIL_SIZE)),
			wantContains: []string{
				fmt.Sprintf("(1, %s, %s)", strings.Repeat("a", COLUMN_USERNAME_SIZE), strings.Repeat("a", COLUMN_EMAIL_SIZE)),
				"Executed.",
			},
			wantRows: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output bytes.Buffer

			tmpFile, err := os.CreateTemp("", "test_db_*.db")
			if err != nil {
				t.Fatalf("failed to create temp file: %v", err)
			}
			tmpFileName := tmpFile.Name()
			tmpFile.Close()

			defer os.Remove(tmpFileName)

			table := dbOpen(tmpFileName)

			runREPL(strings.NewReader(tt.input), &output, table)
			got := output.String()
			for _, want := range tt.wantContains {
				if !strings.Contains(got, want) {
					t.Errorf("%s: output missing expected part %q\ngot:\n%s", tt.name, want, got)
				}
			}
			if table.numRows != uint32(tt.wantRows) {
				t.Errorf("%s: table.numRows = %d, want %d", tt.name, table.numRows, tt.wantRows)
			}
		})
	}
}
