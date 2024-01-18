package main

import (
	"fmt"
	"os"
	// Available if you need it!
	/* "github.com/xwb1989/sqlparser" */)

// Usage: your_sqlite3.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
    _ = DBInfoCmd(databaseFilePath)
	case ".tables":
    _ = DBTablesCmd(databaseFilePath)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
