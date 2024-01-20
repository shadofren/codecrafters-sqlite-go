package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/xwb1989/sqlparser"
)

const (
	InteriorIndexPage = 0x02
	InteriorTablePage = 0x05
	LeafIndexPage     = 0x0a
	LeafTablePage     = 0x0d
)

const (
	NULL    = 0x00
	INT8    = 0x01
	INT16   = 0x02
	INT24   = 0x03
	INT32   = 0x04
	INT48   = 0x05
	INT64   = 0x06
	FLOAT64 = 0x07
	ZERO    = 0x08
	ONE     = 0x09
	BLOB    = 0xc
	TEXT    = 0xd
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

type InteriorTableCell struct {
	LeftChildPage int64 // actually only 4 byte but to be consistent
	RowId         int64 // varint
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

func DBSelectCmd(dbPath string, stmt *sqlparser.Select) {
	tableExpr := stmt.From[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName)
	tableName := tableExpr.Name.String()

	info, _ := readInfo(dbPath)
	for _, table := range info.Tables {
		if table.Name != tableName {
			continue
		}

		isCount := false
		outputCols := make([]string, 0)
		/* should be an AliasedExpr, work on 1 column right now */
		for _, expr := range stmt.SelectExprs {
			expr := expr.(*sqlparser.AliasedExpr).Expr
			if _, ok := expr.(*sqlparser.FuncExpr); ok {
				isCount = true
				break
			}
			colName := expr.(*sqlparser.ColName).Name.String()
			outputCols = append(outputCols, colName)
		}

		_, cells := readPage(dbPath, info, table.RootPage)
		if isCount {
			fmt.Println(len(cells))
			return
		}

		sql := strings.ReplaceAll(table.Sql, "autoincrement", "")
		sql = strings.ReplaceAll(sql, "\"", "") // table name has doublequote
		tableSql, err := sqlparser.Parse(sql)
		must(err)
		// the columns in order, then we parse the data
		columns := tableSql.(*sqlparser.DDL).TableSpec.Columns
		tableCols := make(map[string]int)
		for i, col := range columns {
			tableCols[col.Name.String()] = i
		}

    // filter the cells 
    filteredCells := make([][]any, 0)
    _ = filteredCells
    for _, cell := range cells {
      _ = cell
      if filter(cell, tableCols, stmt.Where) {
        filteredCells = append(filteredCells, cell)
      }
    }

		results := make([][]string, len(filteredCells))

		for _, outCol := range outputCols {
			if idx, ok := tableCols[outCol]; ok {
				for i, row := range filteredCells {
					switch r := row[idx].(type) {
					case int64:
						results[i] = append(results[i], strconv.FormatInt(r, 10))
					case string:
						results[i] = append(results[i], r)
          default:
            fmt.Println("not supported type")
					}
				}
			}
		}

		for _, res := range results {
			fmt.Println(strings.Join(res, "|"))
		}
	}
}

func filter(row []any, tableCols map[string]int, where *sqlparser.Where) bool {
	if where == nil {
		return true
	}

	if where, ok := where.Expr.(*sqlparser.ComparisonExpr); ok {
		// assume to be equal
		colName := where.Left.(*sqlparser.ColName).Name.String()
		colValue := string(where.Right.(*sqlparser.SQLVal).Val)
		if idx, ok := tableCols[colName]; ok {
			value := row[idx]
			return value == colValue
		}
	}
	return false
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
		row := readTableLeafCell(reader)
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

func readPage(dbPath string, dbinfo *DBInfo, page int64) (*BTreePageHeader, [][]any) {
	dbFile, err := os.Open(dbPath)
	must(err)
	defer dbFile.Close()

	offset := (page - 1) * int64(dbinfo.PageSize)

	dbFile.Seek(offset, io.SeekStart)
	content := make([]byte, dbinfo.PageSize)
	dbFile.Read(content)

	reader := bytes.NewReader(content)
	pageHeader, _ := readPageHeader(reader)

	cellStarts := []int64{}
	// start of cell pointer array
	for i := 0; i < int(pageHeader.NumberOfCells); i++ {
		var p uint16
		binary.Read(reader, binary.BigEndian, &p)
		cellStarts = append(cellStarts, int64(p))
	}
	cells := make([][]any, 0)
	switch pageHeader.PageType {
	case LeafTablePage:
		for _, offset := range cellStarts {
			reader.Seek(offset, io.SeekStart)
			cell := readTableLeafCell(reader)
			cells = append(cells, cell)
		}
	case InteriorTablePage:
		interiorCells := make([]*InteriorTableCell, 0)
		for _, offset := range cellStarts {
			reader.Seek(offset, io.SeekStart)
			cell := readTableInteriorCell(reader)
			interiorCells = append(interiorCells, cell)
		}
		// recursion to get the actual cell
		for _, cell := range interiorCells {
			_, rows := readPage(dbPath, dbinfo, cell.LeftChildPage)
			cells = append(cells, rows...)
		}
	default:
		fmt.Println("index not supported")
	}
	return pageHeader, cells
}

func readTableInteriorCell(reader *bytes.Reader) *InteriorTableCell {

	cell := &InteriorTableCell{}
	var leftPage uint32 // 4 byte int
	binary.Read(reader, binary.BigEndian, &leftPage)
	cell.LeftChildPage = int64(leftPage)
	rowId, _ := readVarint(reader)
	cell.RowId = rowId
	return cell
}

func readTableLeafCell(reader *bytes.Reader) []any {
	_, _ = readVarint(reader)
	rowId, _ := readVarint(reader)
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
			// this is place holder for the row Id primary index?
			data = append(data, nil)
		case INT8:
			v, _ := reader.ReadByte()
			data = append(data, int64(v))
		case INT16:
			var v uint16
			err := binary.Read(reader, binary.BigEndian, &v)
			must(err)
			data = append(data, int64(v))
		case INT24:
			fmt.Println("INT24")
		case INT32:
			fmt.Println("INT32")
		case INT48:
			fmt.Println("INT48")
		case INT64:
			fmt.Println("INT64")
		case FLOAT64:
			fmt.Println("FLOAT64")
		case ZERO:
			data = append(data, 0)
		case ONE:
			data = append(data, 1)
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
  data[0] = rowId
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
