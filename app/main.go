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
    dbinfo := DBInfoCmd(databaseFilePath)
		fmt.Printf("database page size: %v", dbinfo.PageSize)
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}
