package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

const (
	InteriorIndexPage = 0x02
	InteriorTablePage = 0x05
	LeafIndexPage     = 0x0a
	LeafTablePage     = 0x0d
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

	fmt.Println("number of tables: ", pageHeader.NumberOfCells)

	/* unused := int(cellStarts[pageHeader.NumberOfCells-1]) - offset */
	/* reader.Discard(unused) */
	/**/
	/* offset += unused */

	/* lastByte := int(dbinfo.PageSize - uint16(dbinfo.ReservedSpace)) */
	/*  U := lastByte - 100 // first page  */
	/**/
	/* cellData := make([]byte, lastByte-offset) */
	/* fmt.Println("all cells", cellStarts) */
	/* fmt.Println("offset", offset) */
	/* reader.Read(cellData) */
	/* for _, idx := range cellStarts { */
	/*    offset = int(pageHeader.StartOfCellContent) */
	/*    cell := cellData[int(idx)-offset:lastByte-offset] */
	/* 	lastByte = int(idx) */
	/* } */

	// we are at the page 1, root page which contains the schema table
	// root page has 100 byte less storage due to presence of header
	return dbinfo
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
