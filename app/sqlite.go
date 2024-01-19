package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
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
	Tables        []*SQLiteSchema
}

type SQLiteSchema struct {
	Type     string
	Name     string
	TblName  string
	RootPage int64
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

func DBInfoCmd(dbPath string) {
	info, pageHeader := readInfo(dbPath)

	fmt.Println("database page size:", info.PageSize)
	fmt.Println("number of tables:", pageHeader.NumberOfCells)
}

func DBTablesCmd(dbPath string) {
	info, _ := readInfo(dbPath)
	for _, table := range info.Tables {
		fmt.Printf("%s ", table.TblName)
	}
	fmt.Println()
}

func DBCountRowCmd(dbPath string, tablename string) {
	info, _ := readInfo(dbPath)
	for _, table := range info.Tables {
		if table.Name == tablename {

      pageHeader := readPage(dbPath, info, table.RootPage)
      fmt.Println(pageHeader.NumberOfCells)
		}
	}
}

func readInfo(dbPath string) (*DBInfo, *BTreePageHeader) {

	dbFile, err := os.Open(dbPath)
	must(err)

	defer dbFile.Close()

	header := make([]byte, 100)

	_, err = dbFile.Read(header)
	must(err)

	dbinfo := &DBInfo{Header: header[:12]}

	err = binary.Read(bytes.NewReader(header[16:18]), binary.BigEndian, &dbinfo.PageSize)
	must(err)

	dbinfo.ReservedSpace = header[20]

	// create buffer for full page
	pageContent := make([]byte, dbinfo.PageSize)
	_, err = dbFile.Read(pageContent[100:]) // read the content, skipping the header
	must(err)

	reader := bytes.NewReader(pageContent)
	reader.Seek(100, io.SeekStart) // skip the first 100 bytes of header

	pageHeader, _ := readPageHeader(reader)

	cellStarts := []int64{}
	// start of cell pointer array
	for i := 0; i < int(pageHeader.NumberOfCells); i++ {
		var p uint16
		binary.Read(reader, binary.BigEndian, &p)
		cellStarts = append(cellStarts, int64(p))
	}

	tables := make([]*SQLiteSchema, 0)
	for _, idx := range cellStarts {
		reader.Seek(idx, io.SeekStart)
		row := readCell(reader)
		table := &SQLiteSchema{}
		table.Type, _ = row[0].(string)
		table.Name, _ = row[1].(string)
		table.TblName, _ = row[2].(string)
		table.RootPage, _ = row[3].(int64)
		table.Sql, _ = row[4].(string)
		tables = append(tables, table)
	}

	dbinfo.Tables = tables
	return dbinfo, pageHeader
}

func readPage(dbPath string, dbinfo *DBInfo, page int64) *BTreePageHeader {
	dbFile, err := os.Open(dbPath)
	must(err)
	defer dbFile.Close()

	offset := (page - 1) * int64(dbinfo.PageSize)

	dbFile.Seek(offset, io.SeekStart)
	content := make([]byte, dbinfo.PageSize)
	dbFile.Read(content)

	reader := bytes.NewReader(content)
	pageHeader, _ := readPageHeader(reader)

  return pageHeader
	/* switch pageHeader.PageType { */
	/* case LeafTablePage: */
 /*   */
	/* default: */
	/* 	fmt.Println("not supported yet") */
	/* } */
}

func readCell(reader *bytes.Reader) []any {

	readVarint(reader)
	_, _ = readVarint(reader)
	totalHeaderSize, offset := readVarint(reader)
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
			data = append(data, int64(v))
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
				data = append(data, string(text))
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

func readPageHeader(reader *bytes.Reader) (*BTreePageHeader, int) {
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
