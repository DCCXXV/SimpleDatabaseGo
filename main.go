package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type ExecuteResult uint8

const (
	EXECUTE_SUCCESS    ExecuteResult = 0
	EXECUTE_TABLE_FULL ExecuteResult = 1
)

type MetaCommandResult uint8

const (
	META_COMMAND_SUCCESS              MetaCommandResult = 0
	META_COMMAND_UNRECOGNIZED_COMMAND MetaCommandResult = 1
	META_COMMAND_EXIT                 MetaCommandResult = 2
)

type PrepareResult uint8

const (
	PREPARE_SUCCESS                PrepareResult = 0
	PREPARE_UNRECOGNIZED_STATEMENT PrepareResult = 1
	PREPARE_SYNTAX_ERROR           PrepareResult = 2
	PREPARE_STRING_TOO_LONG        PrepareResult = 3
)

type StatementType uint8

const (
	STATEMENT_INSERT StatementType = 0
	STATEMENT_SELECT StatementType = 1
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
)

const (
	ID_SIZE         = 4 // uint32 -> 4 bytes
	USERNAME_SIZE   = COLUMN_USERNAME_SIZE
	EMAIL_SIZE      = COLUMN_EMAIL_SIZE
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
)

type Row struct {
	id       uint32
	username string
	email    string
}

type Statement struct {
	Type        StatementType
	RowToInsert Row
}

func serializeRow(source *Row, destination []byte) {
	destination[0] = byte(source.id)
	destination[1] = byte(source.id >> 8)
	destination[2] = byte(source.id >> 16)
	destination[3] = byte(source.id >> 24)

	usernameBytes := []byte(source.username)
	bytesToCopy := min(len(usernameBytes), USERNAME_SIZE)
	copy(destination[USERNAME_OFFSET:USERNAME_OFFSET+bytesToCopy], usernameBytes[:bytesToCopy])
	for i := USERNAME_OFFSET + bytesToCopy; i < USERNAME_OFFSET+USERNAME_SIZE; i++ {
		destination[i] = 0
	}

	emailBytes := []byte(source.email)
	bytesToCopy = min(len(emailBytes), EMAIL_SIZE)
	copy(destination[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE], source.email)
	for i := EMAIL_OFFSET + bytesToCopy; i < EMAIL_OFFSET+EMAIL_SIZE; i++ {
		destination[i] = 0
	}
}

func deserializeRow(source []byte, destination *Row) {
	destination.id = uint32(source[0]) | uint32(source[1])<<8 | uint32(source[2])<<16 | uint32(source[3])<<24

	usernameBytes := source[USERNAME_OFFSET : USERNAME_OFFSET+USERNAME_SIZE]
	nullIndex := bytes.IndexByte(usernameBytes, 0)
	if nullIndex == -1 {
		destination.username = string(usernameBytes)
	} else {
		destination.username = string(usernameBytes[:nullIndex])
	}

	emailBytes := source[EMAIL_OFFSET : EMAIL_OFFSET+EMAIL_SIZE]
	nullIndex = bytes.IndexByte(emailBytes, 0)
	if nullIndex == -1 {
		destination.email = string(emailBytes)
	} else {
		destination.email = string(emailBytes[:nullIndex])
	}
}

const PAGE_SIZE = 4096
const TABLE_MAX_PAGES = 100
const ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
const TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES

type Page [PAGE_SIZE]byte

type Table struct {
	numRows uint32
	pages   [TABLE_MAX_PAGES]*Page
}

func rowSlot(table *Table, rowNum uint32) []byte {
	pageNum := rowNum / ROWS_PER_PAGE
	page := table.pages[pageNum]

	if page == nil {
		page = new(Page)
		table.pages[pageNum] = page
	}

	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE

	return page[byteOffset : byteOffset+ROW_SIZE]
}

func newTable() *Table {
	table := &Table{
		numRows: 0,
	}

	return table
}

func printRow(row *Row, writer *bufio.Writer) {
	writer.WriteString("(" + strconv.FormatUint(uint64(row.id), 10) + ", " + row.username + ", " + row.email + ")\n")
}

func doMetaCommand(input string) MetaCommandResult {
	if input == "+quit" {
		return META_COMMAND_EXIT
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepareStatement(input string, statement *Statement) PrepareResult {
	if strings.HasPrefix(input, "insert") {
		statement.Type = STATEMENT_INSERT

		var id uint32
		var username, email string

		argsAssigned, err := fmt.Sscanf(input, "insert %d %s %s", &id, &username, &email)

		if err != nil || argsAssigned < 3 {
			return PREPARE_SYNTAX_ERROR
		}

		if len(username) > COLUMN_USERNAME_SIZE {
			return PREPARE_STRING_TOO_LONG
		}

		if len(email) > COLUMN_EMAIL_SIZE {
			return PREPARE_STRING_TOO_LONG
		}

		statement.RowToInsert = Row{
			id:       id,
			username: username,
			email:    email,
		}

		return PREPARE_SUCCESS
	}

	if strings.HasPrefix(input, "select") {
		statement.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.numRows >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}

	rowToInsert := &statement.RowToInsert

	serializeRow(rowToInsert, rowSlot(table, table.numRows))
	table.numRows++

	return EXECUTE_SUCCESS
}

func executeSelect(table *Table, writer *bufio.Writer) ExecuteResult {
	var row Row
	for i := 0; i < int(table.numRows); i++ {
		deserializeRow(rowSlot(table, uint32(i)), &row)
		printRow(&row, writer)
	}

	return EXECUTE_SUCCESS
}

func executeStatement(statement *Statement, table *Table, writer *bufio.Writer) ExecuteResult {
	switch statement.Type {
	case STATEMENT_INSERT:
		return executeInsert(statement, table)
	case STATEMENT_SELECT:
		return executeSelect(table, writer)
	default:
		return EXECUTE_SUCCESS // change
	}
}

func runREPL(input io.Reader, output io.Writer, table *Table) {
	reader := bufio.NewReader(input)
	writer := bufio.NewWriter(output)
	defer writer.Flush()

	for {
		writer.WriteString("simpledbgo > ")
		writer.Flush()
		input, err := reader.ReadString('\n')

		if err != nil {
			if err == io.EOF {
				break
			}
			writer.WriteString("Error reading input:" + err.Error() + "\n")
			break
		}

		command := strings.TrimSpace(input)
		if len(command) == 0 {
			continue
		}

		// meta commands
		if command[0] == '+' {
			switch doMetaCommand(command) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				writer.WriteString("Unrecognized command " + command + ".\n")
				continue
			case META_COMMAND_EXIT:
				return
			}
		}

		// prepare SQL statements
		var statement Statement
		switch prepareStatement(command, &statement) {
		case PREPARE_SUCCESS:
			// exec SQL statements
			switch executeStatement(&statement, table, writer) {
			case (EXECUTE_SUCCESS):
				writer.WriteString("Executed.\n")
			case (EXECUTE_TABLE_FULL):
				writer.WriteString("Error: Table full.\n")
			}
		case PREPARE_UNRECOGNIZED_STATEMENT:
			writer.WriteString("Unrecognized keyword at start of " + command + ".\n")
		case (PREPARE_SYNTAX_ERROR):
			writer.WriteString("Syntax error. Could not parse statement.\n")
		case PREPARE_STRING_TOO_LONG:
			writer.WriteString("String is too long.\n")
		}
	}
}

func main() {
	table := newTable()
	runREPL(os.Stdin, os.Stdout, table)
	os.Exit(0)
}
