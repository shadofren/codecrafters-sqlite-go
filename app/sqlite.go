package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	InteriorIndexPage = 0x02
	InteriorTablePage = 0x05
	LeafIndexPage     = 0x0a
	LeafTablePage     = 0x0d
)

const (
	NULL  = 0x00
	INT8  = 0x01
	INT16 = 0x02
	INT24 = 0x03
	INT32 = 0x04
	// TODO others
	BLOB = 0xc
	TEXT = 0xd
)

type DBInfo struct {
	Header        []byte
	PageSize      uint16
	ReservedSpace uint8 // usually 0, but we need to account for this
}

type SQLiteSchema struct {
	Type     string
	Name     string
	TblName  string
	RootPage uint64
	Sql      string
}

type BTreePageHeader struct {
	PageType                    uint8
	StartOfFirstFree            uint16
	NumberOfCells               uint16
	StartOfCellContent          uint16
	NumberOfFragmentedFreeBytes uint8
	RightMostPointer            uint32
}

func DBInfoCmd(dbPath string) *DBInfo {
	databaseFile, err := os.Open(dbPath)
	must(err)

	defer databaseFile.Close()

	header := make([]byte, 100)
	reader := bufio.NewReader(databaseFile)

	_, err = reader.Read(header)
	must(err)

	dbinfo := &DBInfo{Header: header[:12]}

	err = binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &dbinfo.PageSize)
	must(err)

	dbinfo.ReservedSpace = header[20]
	fmt.Println("reserved space", dbinfo.ReservedSpace)

	pageHeader, n := readPageHeader(reader)
	fmt.Printf("%+v\n", pageHeader) // we have a leaf table pageType 13

	offset := 100 + n // where we are now in the reader
	cellStarts := []uint16{}
	// start of cell pointer array
	for i := 0; i < int(pageHeader.NumberOfCells); i++ {
		var p uint16
		binary.Read(reader, binary.BigEndian, &p)
		fmt.Println("cell", i, "offset", p)
		cellStarts = append(cellStarts, p)
		offset += 2
	}

	fmt.Println("database page size:", dbinfo.PageSize)
	fmt.Println("number of tables:", pageHeader.NumberOfCells)

	return dbinfo
}

func DBTablesCmd(dbPath string) *DBInfo {
	databaseFile, err := os.Open(dbPath)
	must(err)

	defer databaseFile.Close()

	header := make([]byte, 100)
	reader := bufio.NewReader(databaseFile)

	_, err = reader.Read(header)
	must(err)

	dbinfo := &DBInfo{Header: header[:12]}

	err = binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &dbinfo.PageSize)
	must(err)

	dbinfo.ReservedSpace = header[20]

	pageHeader, n := readPageHeader(reader)
	fmt.Printf("%+v\n", pageHeader) // we have a leaf table pageType 13

	offset := 100 + n // where we are now in the reader
	cellStarts := []uint16{}
	// start of cell pointer array
	for i := 0; i < int(pageHeader.NumberOfCells); i++ {
		var p uint16
		binary.Read(reader, binary.BigEndian, &p)
		fmt.Println("cell", i, "offset", p)
		cellStarts = append(cellStarts, p)
		offset += 2
	}

	fmt.Println("number of tables:", pageHeader.NumberOfCells)

	unused := int(cellStarts[pageHeader.NumberOfCells-1]) - offset
	reader.Discard(unused)

	offset += unused

	lastByte := int(dbinfo.PageSize - uint16(dbinfo.ReservedSpace))

	cells := make([][]byte, 0)
	cellData := make([]byte, lastByte-offset)
	reader.Read(cellData)
	for _, idx := range cellStarts {
		offset = int(pageHeader.StartOfCellContent)
		cell := cellData[int(idx)-offset : lastByte-offset]
		cells = append(cells, cell)
		lastByte = int(idx)
	}

	tables := make([]string, 0)
	for _, cell := range cells {
		/* CREATE TABLE sqlite_schema( */
		/*   type text, */
		/*   name text, */
		/*   tbl_name text, */
		/*   rootpage integer, */
		/*   sql text */
		/* ); */
		row := readCell(bytes.NewReader(cell))
		// name of table is at index 1 or 2
		if val, ok := row[2].([]byte); ok {
      tableName := string(val)
      if tableName == "sqlite_sequence" {
        continue
      }
			tables = append(tables, tableName)
		}
	}

	fmt.Println(strings.Join(tables, " "))
	return dbinfo
}

func readCell(reader *bytes.Reader) []any {

	length, _ := readVarint(reader)
	rowId, _ := readVarint(reader)
	totalHeaderSize, offset := readVarint(reader)
	fmt.Println("length is", length)
	fmt.Println("id is", rowId)
	// read the column Type
	colTypes := make([]int64, 0)
	for offset < int(totalHeaderSize) {
		columnType, m := readVarint(reader)
		colTypes = append(colTypes, columnType)
		offset += m
	}

	data := make([]any, 0)
	// read the data
	for _, t := range colTypes {
		switch t {
		case NULL:
			fmt.Println("NULL")
		case INT8:
			v, _ := reader.ReadByte()
			data = append(data, v)
			fmt.Printf("INT8: %d\n", v) // 1 byte
		case INT16:
			fmt.Println("INT16")
		case INT24:
			fmt.Println("INT24")
		case INT32:
			fmt.Println("INT32")
		default:
			if t&1 == 0 {
				fmt.Println("a blob of size", (t-12)/2)
			} else {
				size := (t - 13) / 2
				text := make([]byte, size)
				reader.Read(text)
				data = append(data, text)
			}
		}
	}
	return data
}

var intMask byte = 0x7F

func readVarint(reader *bytes.Reader) (int64, int) {

	byteRead := 1
	var ans int64 = 0
	cur, _ := reader.ReadByte()
	for {
		ans <<= 7
		ans += int64(cur & intMask)
		if (cur >> 7) == 0x00 {
			break
		}
		cur, _ = reader.ReadByte()
		byteRead++
	}

	return ans, byteRead
}

func readPageHeader(reader *bufio.Reader) (*BTreePageHeader, int) {
	header := BTreePageHeader{}
	header.PageType, _ = reader.ReadByte() // single byte
	binary.Read(reader, binary.BigEndian, &header.StartOfFirstFree)
	binary.Read(reader, binary.BigEndian, &header.NumberOfCells)
	binary.Read(reader, binary.BigEndian, &header.StartOfCellContent)
	binary.Read(reader, binary.BigEndian, &header.NumberOfFragmentedFreeBytes)
	n := 8
	if header.PageType == InteriorIndexPage || header.PageType == InteriorTablePage {
		binary.Read(reader, binary.BigEndian, &header.RightMostPointer)
		n += 4
	}
	return &header, n
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
