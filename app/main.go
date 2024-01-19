package main

import (
	"os"
	"strings"
	// Available if you need it!
	/* "github.com/xwb1989/sqlparser" */)

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
    args := strings.Split(command, " ")
    tableName := args[len(args)-1]
    DBCountRowCmd(databaseFilePath, tableName)
	}
}
