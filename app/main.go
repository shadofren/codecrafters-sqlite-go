package main

import (
	"os"

	"github.com/xwb1989/sqlparser"
)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		DBInfoCmd(databaseFilePath)
	case ".tables":
		DBTablesCmd(databaseFilePath)
	default:
		stmt, _ := sqlparser.Parse(command)
		switch stmt := stmt.(type) {
		case *sqlparser.Select:
			DBSelectCmd(databaseFilePath, stmt)
		default:
		}
	}
}
